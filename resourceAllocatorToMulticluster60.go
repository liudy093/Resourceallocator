/**
 * 资源分配器主程序
 * @author Shan chenggang
 * @version date：2020年6月1日  下午8:39:51
 */
package main

import (
	"MultiClusterResourceAllocator60/k8sResource"
	"MultiClusterResourceAllocator60/message/resource_allocator"
	"MultiClusterResourceAllocator60/message/scheduler_controller"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	_ "flag"
	_ "fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	_ "path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	_ "github.com/fsnotify/fsnotify"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	_ "k8s.io/api/apps/v1beta1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/runtime"
	_ "k8s.io/apimachinery/pkg/util/runtime"
	_ "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	v2 "k8s.io/client-go/listers/core/v1"
	_ "k8s.io/client-go/tools/cache"
	_ "k8s.io/client-go/tools/clientcmd"
)

/*访问K8s Api的clientset*/
var clientset *kubernetes.Clientset

/*资源分配结构体*/
type resourceAllocation struct {
	MilliCpu uint64
	Memory   uint64
}

/*资源分配配置结构体*/
type requestResourceConfig struct {
	Timestamp      int64
	AliveStatus    bool
	ResourceDemand resourceAllocation
}

/*资源请求结构体*/
type resourceRequest struct {
	MilliCpu         uint64
	Memory           uint64
	EphemeralStorage uint64
}

/*控制器重启标志位*/
var controllerBackOffFlag bool = false

/*service数量变量*/
var serviceNum uint32 = 0

/*本集群删除的调度器pod数量*/
var deletedSchedulerNum uint32 = 0

/*scheduler数量变量*/
var schedulerPodNum uint64 = 0

/*任务namespace数量变量*/
var taskNsNum uint64 = 0

/*任务pod数量变量*/
var taskPodNum uint64 = 0

var getK8sResource resourceRequest

/*调度器访问资源分配器变量*/
var requestNum uint64

/*资源Lister变量*/
//var clusterPodLister[] v2.PodLister
//var clusterNodeLister[] v2.NodeLister
//var clusterNamespaceLister[] v2.NamespaceLister
//var cluster1NamespaceLister,cluster2NamespaceLister,cluster3NamespaceLister v2.NamespaceLister
var podLister v2.PodLister
var nodeLister v2.NodeLister
var thisClusterNamespaceLister v2.NamespaceLister
var thisClusterServiceLister v2.ServiceLister
var thisClusterPvcLister v2.PersistentVolumeClaimLister

/*各类管道变量*/
var taskSema = make(chan int, 1)
var schedulerSema = make(chan int, 1)
var serviceSema = make(chan int, 1)
var deploySema = make(chan int, 1)

/*集群ip数组变量*/
var clusterIps []string
var nodeResidualMap k8sResource.ResidualResourceMap
var schedulerNumArray [10]uint32
var bootIdArray [10]string
var i uint32 = 0
var masterIp string
var schedulerName string
var clusterId string
var imagePullPolicy string

/*链接Etcd分布式数据库需要的参数*/
var (
	dialTimeout    = 3 * time.Second
	requestTimeout = 2 * time.Second
	//endpoints   = []string{"192.168.0.160:2379","192.168.0.161:2379","192.168.0.162:2379"}
	etcdCert    = "/etc/etcd/ssl/etcd.pem"
	etcdCertKey = "/etc/etcd/ssl/etcd-key.pem"
	etcdCa      = "/etc/kubernetes/ssl/ca.pem"
)

// 资源分配器服务实现结构体
type ResourceServiceImpl struct {
}

/*判断item是否在数组items内*/
//func IsContain(items []string, item string) bool {
//	for _, eachItem := range items {
//		if eachItem == item {
//			return true
//		}
//	}
//	return false
//}
func recoverGetAllocatableSchedulerNumFail() {
	if r := recover(); r != nil {
		log.Println("recovered from recoverGetAllocatableSchedulerNumFail()", r)
	}
}

// 调度器控制器请求可分配的调度器数量
func (rs *ResourceServiceImpl) GetAllocatableSchedulerNum(ctx context.Context, request *resource_allocator.GetSchedulerNumRequest) (*resource_allocator.GetSchedulerNumResponse, error) {
	var response resource_allocator.GetSchedulerNumResponse
	defer recoverGetAllocatableSchedulerNumFail()
	/*如果第二次申请调度器数量SchedulerPodNum等于第一次，只能说明第一次生成的调度器还没有完全running
	通过Informer获取调度器namespace下的调度器pod数量判断，如果无pod running即是len(pods.Items) == 0
	,则第二次SchedulerPodNum置为schedulerNumArray[0].如果已经存在running的scheduler pod,则
	SchedulerPodNum = schedulerNumArray[0] - len(pods.Items).*/
	pods, err := clientset.CoreV1().Pods("scheduler-ns").List(metav1.ListOptions{})
	if err != nil {
		panic(err)
	}
	//获取每个Node的剩余资源,ResidualMap含NODE_NUM个key-value
	//由于podLister, nodeLister只能作用于本集群，
	//故ResidualMap只有本集群NODE_NUM个node节点的有效值
	ResidualMap := k8sResource.GetK8sEachNodeResource(podLister, nodeLister, nodeResidualMap)
	//集群Node节点可参与负载的资源量
	//env方式获取集群Node节点的cpu参与负载的最大值,yaml中configMap方式设置
	nodeMaxCpu, err := strconv.Atoi(os.Getenv("RESOURCE_CPU"))
	if err != nil {
		panic(err)
	}
	//env方式获取集群Node节点的mem参与负载的最大值,yaml中configMap方式设置
	nodeMaxMem, err := strconv.Atoi(os.Getenv("RESOURCE_MEM"))
	if err != nil {
		panic(err)
	}
	cpuResidualResourceMinValue := uint64(nodeMaxCpu)
	memResidualResourceMinValue := uint64(nodeMaxMem)
	for _, val := range ResidualMap {
		//if (val.MilliCpu < cpuResidualResourceMinValue) && (val.MilliCpu != 0) && !IsContain(clusterIps, ip) {
		if (val.MilliCpu < cpuResidualResourceMinValue) && (val.MilliCpu != 0) {
			cpuResidualResourceMinValue = val.MilliCpu
		}
		//if (val.Memory < memResidualResourceMinValue) && (val.Memory != 0) && !IsContain(clusterIps, ip) {
		if (val.Memory < memResidualResourceMinValue) && (val.Memory != 0) {
			memResidualResourceMinValue = val.Memory
		}
	}
	log.Printf("Residual cpu minimum of all nodes is :%d,Residual memory minimum of all nodes is:%d.\n", cpuResidualResourceMinValue, memResidualResourceMinValue)
	//env方式获取集群Node节点数量,yaml中configMap方式设置
	nodeNum, err := strconv.Atoi(os.Getenv("NODE_NUM"))
	if err != nil {
		panic(err)
	}
	cpuNum := int64(float64(cpuResidualResourceMinValue)*0.6/float64(request.Cpu)) * int64(nodeNum)
	memNum := int64(float64(memResidualResourceMinValue)*0.6/float64(request.Mem)) * int64(nodeNum)
	log.Printf("allocatable scheduler pod cpuNum:%d,allocatable memNum:%d.\n", cpuNum, memNum)
	if cpuNum >= memNum {
		response.SchedulerPodNum = memNum
	} else {
		response.SchedulerPodNum = cpuNum
	}
	//云工作流调度器系统运行具有真实负载的工作流,任务pod（1C 64Mi）
	//以往的方案分配调度器数量,分配的工作流任务过多，批量申请资源过大，导致资源分配过早不足1C，无法继续创建任务Pod
	//再此，人为控制发送给控制器的调度器数量，按照一个node一个调度器的原则.
	//如果集群承载的调度器数量大于节点数，则改为每个节点保留一个调度器
	if uint32(response.SchedulerPodNum) >= uint32(nodeNum) {
		schedulerNumArray[i] = uint32(nodeNum)
	} else { //这种情况下可分配的调度器数量为0
		schedulerNumArray[i] = uint32(response.SchedulerPodNum)
	}
	i++
	//如果相邻的两次调度器数量相等，说明上次启动的调度器还未全部running。
	if schedulerNumArray[0] == schedulerNumArray[1] {
		schedulerNumArray[1] = 0
		schedulerNumArray[0] = schedulerNumArray[0] + schedulerNumArray[1]
		/////*调度器控制重启时，controllerBackOffFlag条件为真*/
		//if controllerBackOffFlag {
		//	/*发送给调度器控制器的调度器数量没有全部生成，需要再次发送剩余调度器数量*/
		//	response.SchedulerPodNum = int64(schedulerNumArray[0] - uint32(len(pods.Items)))
		//	//response.SchedulerPodNum = int64(schedulerNumArray[0] - serviceNum)
		//	i = 1
		//	log.Printf("controller restarts. allocatable scheduler pod number:%d.\n",response.SchedulerPodNum)
		//}else {
		/*调度器未重启，为普遍状态*/
		i = 1
		response.SchedulerPodNum = int64(schedulerNumArray[0] - uint32(len(pods.Items)))
		log.Printf("allocatable schedulerPod num.:%d,actual schedulerPod num in NS is:%d,creating schedulerPod num.:%d.\n",
			response.SchedulerPodNum, len(pods.Items), schedulerPodNum)
		return &response, nil
	} else {
		//可分配调度器为0，说明当前无剩余资源
		if schedulerNumArray[1] == 0 {
			response.SchedulerPodNum = 0
			log.Printf("allocatable schedulerPod num.:%d,actual schedulerPod num in NS is:%d,creating schedulerPod num.:%d.\n",
				response.SchedulerPodNum, len(pods.Items), schedulerPodNum)
			i = 1
			return &response, nil
		} else { //按照当前集群节点数，这时候调度器生成数量为6
			if schedulerNumArray[0] > uint32(len(pods.Items)) {
				response.SchedulerPodNum = int64(schedulerNumArray[0] - uint32(len(pods.Items)))
				log.Printf("allocatable schedulerPod num.:%d,actual schedulerPod num in NS is:%d,creating schedulerPod num.:%d.\n",
					response.SchedulerPodNum, len(pods.Items), schedulerPodNum)
				i = 1
				return &response, nil
			} else { //当前集群已达到调度器Pod最大承载量
				//response.SchedulerPodNum = int64(schedulerNumArray[1])
				response.SchedulerPodNum = 0
				log.Printf("allocatable schedulerPod num.:%d,actual schedulerPod num in NS is:%d,creating schedulerPod num.:%d.\n",
					response.SchedulerPodNum, len(pods.Items), schedulerPodNum)
				i = 1
				return &response, nil
			}
		}
	}
}

/*捕获recoverDeleteWfNamespaceFail异常*/
func recoverDeleteWfNamespaceFail() {
	if r := recover(); r != nil {
		log.Println("recovered from recoverDeleteWfNamespaceFail()", r)
	}
}

// 响应调度器删除执行完毕的工作流namespace
func (rs *ResourceServiceImpl) DeleteWorkflowNamespace(ctx context.Context, request *resource_allocator.DeleteWorkflowNamespaceRequest) (*resource_allocator.DeleteWorkflowNamespaceResponse, error) {
	var response resource_allocator.DeleteWorkflowNamespaceResponse
	defer recoverDeleteWfNamespaceFail()
	workflowId := request.WorkflowId
	namespacesClient := clientset.CoreV1().Namespaces()
	//pvcClient := clientService.CoreV1().PersistentVolumeClaims(workflowId)
	//删除此工作流的namespace
	deletePolicy := metav1.DeletePropagationForeground
	if err := namespacesClient.Delete(workflowId, &metav1.DeleteOptions{
		PropagationPolicy: &deletePolicy,
	}); err != nil {
		panic(err)
		response.Result = 0
		return &response, nil
	}
	log.Printf("Deleting Namespaces %s.\n", workflowId)
	response.Result = 1
	return &response, nil
}

/*捕获UpdateSchedulerAliveStatus异常*/
func recoverUpdateSchedulerAliveStatusFail() {
	if r := recover(); r != nil {
		log.Println("recovered from recoverUpdateSchedulerAliveStatusFail()", r)
	}
}

// 响应调度器控制器更新调度器保活信号请求
func (rs *ResourceServiceImpl) UpdateSchedulerAliveStatus(ctx context.Context, request *resource_allocator.UpdateSchedulerAliveStatusRequest) (*resource_allocator.UpdateSchedulerAliveStatusResponse, error) {
	var response resource_allocator.UpdateSchedulerAliveStatusResponse
	var unMarshalData requestResourceConfig
	defer recoverUpdateSchedulerAliveStatusFail()
	//获取clientv3
	log.Println("-------------------------------------------")
	log.Println("Updating scheduler AliveStatus.")
	cli := getClinetv3()
	if cli == nil {
		log.Println("get *clientv3.Client failed.")
	}
	defer cli.Close()
	//读取etcd过程.取值，设置超时为3秒
	Id := "scheduler/" + request.SchedulerId[0:22]
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	resp, err := cli.Get(ctx, Id)
	cancel()
	if err != nil {
		panic(err.Error())
		return nil, nil
	}
	//获取etcd的key-value
	for _, ev := range resp.Kvs {
		log.Printf("%s : %s\n", ev.Key, ev.Value)
		//反序列化得到结构体unMarshalData
		er := json.Unmarshal(ev.Value, &unMarshalData)
		if er != nil {
			panic(er.Error())
			//log.Println("json Marshal is err: ", err)
			return nil, nil
		}
	}
	//获取scheduler的保活信号
	unMarshalData.AliveStatus = request.AliveStatus
	data, err := json.Marshal(unMarshalData)
	if err != nil {
		panic(err.Error())
		//log.Println("json Marshal is err: ", err)
		return nil, nil
	}
	//设置1秒超时，访问etcd有超时控制
	log.Printf("Updating scheduler alive signal:schedulerId:%s,AliveStatus:%t.\n", Id, request.AliveStatus)
	ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
	//写入etcd key-value
	_, err = cli.Put(ctx, Id, string(data))
	//操作完毕，取消etcd
	cancel()
	if err != nil {
		panic(err.Error())
		return nil, nil
	}
	log.Println("-------------------------------------------")
	//删除scheduler-ns名称空间已经死亡的调度器,先获取podLister
	podList, err := podLister.Pods("scheduler-ns").List(labels.Everything())
	if err != nil {
		log.Println(err)
	}
	//删除集群中已经completed的调度器pod，同时删除该调度器Pod对应的service，注意serviceNum与schedulerPodNum不减
	//为了避免与存在的调度器号和service号的重复，serviceNum与schedulerPodNum不减。
	for _, pod := range podList {
		if pod.Status.Phase == "Succeeded" {
			err = clientset.CoreV1().Pods("scheduler-ns").Delete(pod.Name, &metav1.DeleteOptions{})
			log.Printf("Deleting scheduler podName: %s, podStatus: %s.\n", pod.Name, pod.Status.Phase)
			deletedSchedulerNum++
			log.Printf("This is the %dth deleted scheduler pod.\n", deletedSchedulerNum)
			if err != nil {
				continue
			}
			//拆分pod.Name得到serviceName
			splitName := strings.Split(pod.Name, "-")
			svName := "scheduler-svc-" + splitName[len(splitName)-1]
			err = clientset.CoreV1().Services("scheduler-ns").Delete(svName, &metav1.DeleteOptions{})
			if err != nil {
				continue
			}
			log.Printf("Deleting scheduler serviceName: %s.\n", svName)
		}
	}
	response.Result = 1
	response.ErrNo = 0
	return &response, nil
}

// 响应调度器资源分配请求
func (rs *ResourceServiceImpl) GetResourceAllocateInfo(ctx context.Context, request *resource_allocator.ResourceRequest) (*resource_allocator.ResourceAllocateInfo, error) {

	var response resource_allocator.ResourceAllocateInfo
	currentTime := time.Now().Unix()
	if request.TimeStamp > currentTime {
		response = resource_allocator.ResourceAllocateInfo{SchedulerId: "202001010000",
			CurrentRequest:       &resource_allocator.ResourceAllocateInfoResourceDemand{Cpu: 0, Mem: 0},
			CurrentRequestStatus: false}
		log.Printf("\n SchedulerId %s,request time is bigger than system currentTime. Error.\n", request.SchedulerId)
	} else {
		//剩余资源需要与CurrentRequest资源需求数量比较，结合资源分配算法，给出返回
		allocateResource := getResidualResource(request)
		//server端记录client访问的总次数
		requestNum++
		log.Printf("grpc Request times of server is %d.\n", requestNum)
		//log.Println(allocateResource)
		return allocateResource, nil
	}
	return &response, nil
}

// 响应任务请求创建任务pod
func (rs *ResourceServiceImpl) CreateTaskPod(ctx context.Context, request *resource_allocator.CreateTaskPodRequest) (*resource_allocator.CreateTaskPodResponse, error) {
	var taskPodResponse resource_allocator.CreateTaskPodResponse

	taskSema <- 1
	//clientNamespace, isFirstPod, clientPvcOfThisNamespace,clientSa,err := createTaskPodNamespaces(request, clientset)
	clientNamespace, isFirstPod, clientPvcOfThisNamespace, err := createTaskPodNamespaces(request, clientset)
	if err != nil {
		//panic(err.Error())
		taskPodResponse.ErrNo = 0
		taskPodResponse.Result = 0
		log.Printf("Creating pod failure: %v,%v\n", request.WorkflowId, request.TaskName)
		log.Println("Sending pod failure flag to scheduler......")
		return &taskPodResponse, nil
	}
	<-taskSema

	//volumePath, err := clientTaskCreatePod(request,clientset, clientNamespace,isFirstPod,clientPvcOfThisNamespace,clientSa)
	volumePath, err := clientTaskCreatePod(request, clientset, clientNamespace, isFirstPod, clientPvcOfThisNamespace)
	taskPodResponse.VolumePath = volumePath
	if err != nil {
		//panic(err.Error())
		//由于访问etcd timeout,判断该任务pod是否已经创建成功
		//_, err = podLister.Pods(request.WorkflowId).Get(os.Getenv("CLUSTER_ID") + "-" + request.WorkflowId + "-" + request.TaskName)
		podName := clusterId + "-" + request.WorkflowId + "-" + request.TaskName
		podObject, err := clientset.CoreV1().Pods(request.WorkflowId).Get(podName, metav1.GetOptions{})
		if err != nil {
			taskPodResponse.ErrNo = 0
			taskPodResponse.Result = 0
			log.Printf("Access etcd again, creating pod failure: %v,%v\n", request.WorkflowId, request.TaskName)
			log.Println("Sending pod failure flag to scheduler......")
			return &taskPodResponse, nil
		} else {
			taskPodResponse.Result = 1
			log.Printf("Access etcd again, creating pod successful: %v.\n", podObject.Name)
			taskPodNum++
			log.Printf("This is %dth workflow-task pod.\n", taskPodNum)
			log.Println("Sending pod successful flag to scheduler......")
			return &taskPodResponse, nil
		}
	}
	taskPodResponse.Result = 1
	log.Println("Sending pod successful flag to scheduler......")
	return &taskPodResponse, nil
}

/*捕获createTaskPodNamespaces异常*/
func recoverNamespaceListerFail() {
	if r := recover(); r != nil {
		log.Println("recovered from ", r)
	}
}

// 生成namespace函数
// func createTaskPodNamespaces(request *resource_allocator.CreateTaskPodRequest,clientService *kubernetes.Clientset,
// )(*v1.Namespace, bool, *v1.PersistentVolumeClaim,*v1.ServiceAccount,error) {
func createTaskPodNamespaces(request *resource_allocator.CreateTaskPodRequest, clientService *kubernetes.Clientset,
) (*v1.Namespace, bool, *v1.PersistentVolumeClaim, error) {
	//recover错误捕获
	defer recoverNamespaceListerFail()
	//获取WorkflowId作为namespace的name
	name := request.WorkflowId
	namespacesClient := clientService.CoreV1().Namespaces()
	//pvcClient := clientService.CoreV1().PersistentVolumeClaims(name)
	//saClient := clientService.CoreV1().ServiceAccounts(name)
	/*获取本集群thisClusterNamespaceLister,
	此调度器处理的工作流生成在本集群，隶属于本工作流的所有任务在本集群。
	由于任务约束生成于本集群，故检测本集群的namespaceList即可*/
	namespaceList, err := thisClusterNamespaceLister.List(labels.Everything())
	if err != nil {
		log.Println(err)
	}
	//namespaceList2, err := cluster2NamespaceLister.List(labels.Everything())
	//if err != nil {
	//	log.Println(err)
	//}
	//namespaceList3, err := cluster3NamespaceLister.List(labels.Everything())
	//if err != nil {
	//	log.Println(err)
	//}
	namespaceExist := false
	//遍历namespace的切片namespaceList1
	for _, ns := range namespaceList {
		if ns.Name == name {
			namespaceExist = true
			break
		}
	}
	if namespaceExist {
		//从Informer的Lister获取namespace资源
		nsClientObject, err := thisClusterNamespaceLister.Get(request.WorkflowId)
		if err != nil {
			//panic(err)
			log.Println(err)
			return nil, false, nil, err
		}
		log.Printf("This namespace %v is already exist.\n", name)
		//从Informer的Lister获取pvc资源
		pvcObjectAlreadyInThisNamespace, err := thisClusterPvcLister.PersistentVolumeClaims(request.WorkflowId).Get(name + "-pvc")
		if err != nil {
			//panic(err)
			log.Println(err)
			return nil, false, nil, err
		}
		log.Printf("This pvc %v is already exist.\n", pvcObjectAlreadyInThisNamespace.Name)
		//saObjectInThisNamespace, err := saClient.Get("default",metav1.GetOptions{})
		//if err != nil {
		//	//panic(err)
		//	log.Println(err)
		//	return nil, false, nil, err
		//}
		//log.Printf("This serviceAccount %v is already exist.\n", saObjectInThisNamespace.Name)
		//该namespace已经存在，已经存在pod，podIsFirstInThisNamespace条件为false
		podIsFirstInThisNamespace := false
		//return nsClientObject, podIsFirstInThisNamespace, pvcObjectAlreadyInThisNamespace,saObjectInThisNamespace,nil
		return nsClientObject, podIsFirstInThisNamespace, pvcObjectAlreadyInThisNamespace, nil
	} else {
		//如果不存在此namespace，则创建，然后返回
		namespace := &v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:   name,
				Labels: map[string]string{"namespace": "task"},
			},
			Status: v1.NamespaceStatus{
				Phase: v1.NamespaceActive,
			},
		}
		//serviceAccount
		//serviceAccount := &v1.ServiceAccount{
		//	ObjectMeta: metav1.ObjectMeta{
		//		Name: "default",
		//		Namespace: name,
		//	},
		//}
		clientNamespaceObject, err := namespacesClient.Create(namespace)
		podIsFirstInThisNamespace := true
		if err != nil {
			//panic(err)
			log.Println(err)
			return nil, false, nil, err
		}
		//namespace数量++
		taskNsNum++
		// 创建一个新的Namespaces
		log.Printf("Creating Namespaces...,this is %dth namespace.\n", taskNsNum)
		//创建该namespace下的pvc
		pvcObjectInThisNamespace, err := createPvc(clientService, name)
		if err != nil {
			//panic(err)
			log.Println(err)
			return nil, false, nil, err
		}
		log.Printf("Creating Pvc in Namespaces:%s.\n", clientNamespaceObject.Name)
		//创建该namespace下的serviceAccount
		//saObjectInThisNamespace,err := saClient.Create(serviceAccount)
		//if err != nil {
		//	//panic(err)
		//	log.Println(err)
		//	return nil, false, nil, err
		//}
		//log.Printf("Creating serviceAccount in Namespaces:%s.\n",clientNamespaceObject.Name)
		//return clientNamespaceObject,podIsFirstInThisNamespace, pvcObjectInThisNamespace, saObjectInThisNamespace,nil
		return clientNamespaceObject, podIsFirstInThisNamespace, pvcObjectInThisNamespace, nil
	}
}

func recoverCreatePvcFail() {
	if r := recover(); r != nil {
		log.Println("recovered from ", r)
	}
}

/*创建Pvc函数*/
func createPvc(clientService *kubernetes.Clientset, nameOfNamespace string) (*v1.PersistentVolumeClaim, error) {

	defer recoverCreatePvcFail()
	//创建该namespace下的pvc
	storageClassName := "nfs-storage"
	pvcClient := clientService.CoreV1().PersistentVolumeClaims(nameOfNamespace)
	pvc := new(v1.PersistentVolumeClaim)
	pvc.TypeMeta = metav1.TypeMeta{Kind: "PersistentVolumeClaim", APIVersion: "v1"}
	pvc.ObjectMeta = metav1.ObjectMeta{Name: nameOfNamespace + "-pvc",
		Labels:    map[string]string{"pvc": "nfs"},
		Namespace: nameOfNamespace}
	/*如果是scheduler-ns,pvc存储为10Mi,否则为工作流namespace,pvc存储为5Mi,*/
	if nameOfNamespace == "scheduler-ns" {
		pvc.Spec = v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteMany,
			},
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceStorage: resource.MustParse("10" + "Mi"),
				},
			},
			StorageClassName: &storageClassName,
		}
	} else {
		pvc.Spec = v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteMany,
			},
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceStorage: resource.MustParse("5" + "Mi"),
				},
			},
			StorageClassName: &storageClassName,
		}
	}
	// 产生指定namespace名称下的pvc
	pvcResult, err := pvcClient.Create(pvc)
	if err != nil {
		//panic(err)
		log.Println(err)
		return nil, err
	}
	//log.Printf("Creating Pvc %s on %s\n", pvcResult.ObjectMeta.Name,
	//	pvcResult.ObjectMeta.CreationTimestamp)
	return pvcResult, nil
}

/*捕获clientTaskCreatePod异常*/
func recoverCreatePodFail() {
	if r := recover(); r != nil {
		log.Println("recovered from ", r)
	}
}

// 解析错误的input_vector
func ParseEnvVarsFromInputVector(inputVectors []string) (map[string]string, error) {
	envVars := make(map[string]string)

	// 正则表达式用于匹配键和值
	re := regexp.MustCompile(`\\x[0-9a-fA-F]{2}(.*?)\\x12\\x0[1-3](\d+)`)

	for _, input := range inputVectors {
		// 尝试找到匹配的键值对
		matches := re.FindAllStringSubmatch(input, -1)

		for _, match := range matches {
			if len(match) >= 3 {
				// 第一组捕获为键，第二组捕获为值
				key := match[1]
				value := match[2]
				envVars[key] = value
			}
		}
	}

	return envVars, nil
}

// 生成任务pod函数
// func clientTaskCreatePod(request *resource_allocator.CreateTaskPodRequest ,clientService *kubernetes.Clientset,
//
//	podNamespace *v1.Namespace, IsfirstPod bool,pvcClient *v1.PersistentVolumeClaim,saClient *v1.ServiceAccount)(string, error){
func clientTaskCreatePod(request *resource_allocator.CreateTaskPodRequest, clientService *kubernetes.Clientset,
	podNamespace *v1.Namespace, IsfirstPod bool, pvcClient *v1.PersistentVolumeClaim) (string, error) {
	//golang异常处理机制，利用recover捕获panic
	defer recoverCreatePodFail()
	//schedulerName := os.Getenv("SCHEDULER")
	//schedulerName := "deepk8s-scheduler"
	taskPodName := clusterId + "-" + request.WorkflowId + "-" + request.TaskName
	//首次创建pod，写入volumes的挂载路径到pod
	//否则，从调度器grpc client env获取
	if IsfirstPod {
		//hostPath := "/nfs/data/"
	}
	/*request中的InputVector和OutputVector数组，封装到MAP中，序列化为Key-Value注入任务pod*/
	inputVector := request.InputVector
	outputVector := request.OutputVector
	taskInputOutputDataMap := map[string][]string{
		"input":  inputVector,
		"output": outputVector,
	}
	data, err := json.Marshal(taskInputOutputDataMap)

	//request scheduler有问题，环境变量在InputVector中，Env为空
	parsedEnvVars, err := ParseEnvVarsFromInputVector(request.InputVector)
	if err != nil {
		log.Println("ParseEnvVarsFromInputVector failed")
		return "", err
	}
	log.Printf("InputVector: %+v\n", request.InputVector)
	log.Printf("Parsed envVars: %+v\n", parsedEnvVars)

	if err != nil {
		//log.Println("json Marshal is err: ", err)
		panic(err)
	}
	// 创建task pod
	pod := new(v1.Pod)
	pod.TypeMeta = metav1.TypeMeta{Kind: "Pod", APIVersion: "v1"}
	pod.ObjectMeta = metav1.ObjectMeta{Name: taskPodName, Namespace: podNamespace.Name,
		Labels:      map[string]string{"app": "task"},
		Annotations: map[string]string{"multicluster.admiralty.io/elect": ""}}
	//所有的pod的容器内挂载volume路径为/nfs/data/
	volumePathInContainer := "/nfs/data/"

	//使用自适应资源分配算法
	if os.Getenv("RESOURCE_ALGORITHM") == "adaptive" {
		cpuNum, memNum := adapResourceAllocateAlgorithm(request)

		pod.Spec = v1.PodSpec{
			SchedulerName: schedulerName,
			//RestartPolicy: v1.RestartPolicyAlways,
			RestartPolicy: v1.RestartPolicyNever,
			Volumes: []v1.Volume{
				v1.Volume{
					Name: "pod-share-volume",
					VolumeSource: v1.VolumeSource{
						PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
							ClaimName: pvcClient.ObjectMeta.Name,
						},
					},
				},
			},
			//ServiceAccountName: saClient.Name,
			ServiceAccountName: "default",
			Containers: []v1.Container{
				v1.Container{
					Name:            "task-pod",
					Image:           request.Image,
					ImagePullPolicy: v1.PullIfNotPresent,
					Ports: []v1.ContainerPort{
						v1.ContainerPort{
							ContainerPort: 80,
							Protocol:      v1.ProtocolTCP,
						},
					},
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse(strconv.Itoa(int(cpuNum)) + "m"),
							v1.ResourceMemory: resource.MustParse(strconv.Itoa(int(memNum)) + "Mi"),
						},
						Limits: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse(strconv.Itoa(int(cpuNum)) + "m"),
							v1.ResourceMemory: resource.MustParse(strconv.Itoa(int(memNum)) + "Mi"),
						},
					},
					VolumeMounts: []v1.VolumeMount{
						v1.VolumeMount{
							Name:      "pod-share-volume",
							MountPath: volumePathInContainer,
							//SubPath: pvcClient.ObjectMeta.Name,
						},
					},
					Env: []v1.EnvVar{
						v1.EnvVar{
							Name:  "VOLUME_PATH",
							Value: volumePathInContainer,
						},
						v1.EnvVar{
							Name:  "ENV_MAP",
							Value: string(data),
						},
					},
				},
			},
		}
		log.Println("This is a normal task pod with no user customization.")
	}

	//若customization=true且cost_grade=false，则pod服务质量为Guaranteed，即request=limit；
	//用户定制化工作流，且对花费无要求
	if request.Customization == true {
		if request.CostGrade == false {
			pod.Spec = v1.PodSpec{
				SchedulerName: schedulerName,
				//RestartPolicy: v1.RestartPolicyAlways,
				RestartPolicy: v1.RestartPolicyNever,
				Volumes: []v1.Volume{
					v1.Volume{
						Name: "pod-share-volume",
						VolumeSource: v1.VolumeSource{
							PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
								ClaimName: pvcClient.ObjectMeta.Name,
							},
						},
					},
				},
				//ServiceAccountName: saClient.Name,
				ServiceAccountName: "default",
				Containers: []v1.Container{
					v1.Container{
						Name:            "task-pod",
						Image:           request.Image,
						ImagePullPolicy: v1.PullIfNotPresent,
						Ports: []v1.ContainerPort{
							v1.ContainerPort{
								ContainerPort: 80,
								Protocol:      v1.ProtocolTCP,
							},
						},
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse(strconv.Itoa(int(request.Cpu)) + "m"),
								v1.ResourceMemory: resource.MustParse(strconv.Itoa(int(request.Mem)) + "Mi"),
							},
							Limits: v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse(strconv.Itoa(int(request.Cpu)) + "m"),
								v1.ResourceMemory: resource.MustParse(strconv.Itoa(int(request.Mem)) + "Mi"),
							},
						},
						VolumeMounts: []v1.VolumeMount{
							v1.VolumeMount{
								Name:      "pod-share-volume",
								MountPath: volumePathInContainer,
								//SubPath: pvcClient.ObjectMeta.Name,
							},
						},
						Env: []v1.EnvVar{
							v1.EnvVar{
								Name:  "VOLUME_PATH",
								Value: volumePathInContainer,
							},
							v1.EnvVar{
								Name:  "ENV_MAP",
								Value: string(data),
							},
						},
					},
				},
			}
			log.Println("This is a customized task pod that doesn't care about cost.")
		} else { //cost_grade=true，用户在意花费，则pod服务质量为Burstable，只设置request
			pod.Spec = v1.PodSpec{
				SchedulerName: schedulerName,
				//RestartPolicy: v1.RestartPolicyAlways,
				RestartPolicy: v1.RestartPolicyNever,
				Volumes: []v1.Volume{
					v1.Volume{
						Name: "pod-share-volume",
						VolumeSource: v1.VolumeSource{
							PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
								ClaimName: pvcClient.ObjectMeta.Name,
							},
						},
					},
				},
				//ServiceAccountName: saClient.Name,
				ServiceAccountName: "default",
				Containers: []v1.Container{
					v1.Container{
						Name:            "task-pod",
						Image:           request.Image,
						ImagePullPolicy: v1.PullIfNotPresent,
						Ports: []v1.ContainerPort{
							v1.ContainerPort{
								ContainerPort: 80,
								Protocol:      v1.ProtocolTCP,
							},
						},
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse(strconv.Itoa(int(request.Cpu)) + "m"),
								v1.ResourceMemory: resource.MustParse(strconv.Itoa(int(request.Mem)) + "Mi"),
							},
							//Limits: v1.ResourceList{
							//	v1.ResourceCPU:    resource.MustParse(strconv.Itoa(int(request.Cpu)) + "m"),
							//	v1.ResourceMemory: resource.MustParse(strconv.Itoa(int(request.Mem)) + "Mi"),
							//},
						},
						VolumeMounts: []v1.VolumeMount{
							v1.VolumeMount{
								Name:      "pod-share-volume",
								MountPath: volumePathInContainer,
								//SubPath: pvcClient.ObjectMeta.Name,
							},
						},
						Env: []v1.EnvVar{
							v1.EnvVar{
								Name:  "VOLUME_PATH",
								Value: volumePathInContainer,
							},
							v1.EnvVar{
								Name:  "ENV_MAP",
								Value: string(data),
							},
						},
					},
				},
			}
			log.Println("This is a customized task pod that cares about cost.")
		}
	} else { //若customization=false,用户无定制化工作流，按照原来的策略，则pod服务质量为Guaranteed，即request=limit；
		// 初始化Pod中容器的环境变量map
		envVars := []v1.EnvVar{
			{
				Name:  "VOLUME_PATH",
				Value: volumePathInContainer,
			},
			//{
			//	Name:  "ENV_MAP",
			//	Value: string(data),
			//},
		}
		// 将解析的环境变量map添加到envVars中
		for name, value := range parsedEnvVars {
			envVar := v1.EnvVar{
				Name:  name,
				Value: value,
			}
			envVars = append(envVars, envVar)
		}
		//输出envVars
		log.Printf("This is envVars %+v\n", envVars)

		// 创建非定制化的任务Pod
		pod.Spec = v1.PodSpec{
			SchedulerName: schedulerName,
			//RestartPolicy: v1.RestartPolicyAlways,
			RestartPolicy: v1.RestartPolicyNever,
			Volumes: []v1.Volume{
				v1.Volume{
					Name: "pod-share-volume",
					VolumeSource: v1.VolumeSource{
						PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
							ClaimName: pvcClient.ObjectMeta.Name,
						},
					},
				},
			},
			//ServiceAccountName: saClient.Name,
			ServiceAccountName: "default",
			Containers: []v1.Container{
				v1.Container{
					Name:            "task-pod",
					Image:           request.Image,
					ImagePullPolicy: v1.PullIfNotPresent,
					Ports: []v1.ContainerPort{
						v1.ContainerPort{
							ContainerPort: 80,
							Protocol:      v1.ProtocolTCP,
						},
					},
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse(strconv.Itoa(int(request.Cpu)) + "m"),
							v1.ResourceMemory: resource.MustParse(strconv.Itoa(int(request.Mem)) + "Mi"),
						},
						Limits: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse(strconv.Itoa(int(request.Cpu)) + "m"),
							v1.ResourceMemory: resource.MustParse(strconv.Itoa(int(request.Mem)) + "Mi"),
						},
					},
					VolumeMounts: []v1.VolumeMount{
						v1.VolumeMount{
							Name:      "pod-share-volume",
							MountPath: volumePathInContainer,
							//SubPath: pvcClient.ObjectMeta.Name,
						},
					},
					Env: envVars,
				},
			},
		}
		log.Println("This is a normal task pod with no user customization.")
	}
	_, err = clientService.CoreV1().Pods(podNamespace.Name).Create(pod)
	if err != nil {
		//panic(err.Error())
		log.Println(err)
		return volumePathInContainer, err
	}
	taskPodNum++
	log.Printf("This is %dth workflow-task pod: %v.\n", taskPodNum, pod.ObjectMeta.Name)
	return volumePathInContainer, nil
}

/*捕获CreateSchedulerPod异常*/
func recoverCreateSchedulerPodFail() {
	if r := recover(); r != nil {
		log.Println("recovered from ", r)
	}
}

// 响应调度器控制器请求创建调度器pod
func (rs *ResourceServiceImpl) CreateSchedulerPod(ctx context.Context, request *resource_allocator.CreateSchedulerPodRequest,
) (*resource_allocator.CreateSchedulerPodResponse, error) {
	defer recoverCreateSchedulerPodFail()
	var schedulerPodResponse resource_allocator.CreateSchedulerPodResponse

	schedulerSema <- 1
	clientNamespace, _, clientPvcOfThisNamespace, err := createSchedulerNamespaces(request, clientset)
	if err != nil {
		panic(err.Error())
	}
	// mu.Unlock()
	<-schedulerSema

	serviceSema <- 1
	//创建Service，为了实现跨集群通信
	serviceClient, err := createSchedulerService(request, clientset, clientNamespace)
	if err != nil {
		panic(err.Error())
	}
	<-serviceSema
	/*Deployment方式创建调度器pod,label:app=scheduler*/
	deploySema <- 1
	err = createSchedulerPod(request, clientset, clientNamespace, clientPvcOfThisNamespace,
		serviceClient)
	if err != nil {
		panic(err.Error())
	}
	<-deploySema
	schedulerPodResponse.Result = 1

	return &schedulerPodResponse, nil
}

// 产生namespaces
func createSchedulerNamespaces(request *resource_allocator.CreateSchedulerPodRequest, clientService *kubernetes.Clientset,
) (*v1.Namespace, bool, *v1.PersistentVolumeClaim, error) {
	//创建namespace失败的recover错误捕获机制
	defer recoverNamespaceListerFail()
	//name := request.
	namespaceExist := false
	name := "scheduler-ns"
	namespacesClient := clientService.CoreV1().Namespaces()
	//pvcClient := clientService.CoreV1().PersistentVolumeClaims(name)
	namespaceList, err := thisClusterNamespaceLister.List(labels.Everything())
	if err != nil {
		log.Println(err)
	}
	//遍历本集群的切片namespaceList，如果不存在此namespace，则报错
	for _, ns := range namespaceList {
		if ns.Name == name {
			namespaceExist = true
			break
		}
	}
	if namespaceExist {
		//nsClientObject, err := namespacesClient.Get(name, metav1.GetOptions{})
		nsClientObject, err := thisClusterNamespaceLister.Get(name)
		if err == nil {
			log.Printf("This scheduler namespace %v is already exist.\n", nsClientObject.Name)
		}
		//pvcObjectAlreadyInThisNamespace, err := pvcClient.Get(name+"-pvc", metav1.GetOptions{})
		pvcObjectAlreadyInThisNamespace, err := thisClusterPvcLister.PersistentVolumeClaims(name).Get(name + "-pvc")
		if err == nil {
			log.Printf("This scheduler pvc %v is already exist.\n", pvcObjectAlreadyInThisNamespace.Name)
		}
		//该namespace已经存在，已经存在pod，podIsFirstInThisNamespace条件为false
		podIsFirstInThisNamespace := false
		return nsClientObject, podIsFirstInThisNamespace, pvcObjectAlreadyInThisNamespace, nil
	} else {
		//如果不存在此namespace，则创建，然后返回
		//设置label，供multicluster-scheduler调度使用
		namespace := &v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: name,
				//Labels: map[string]string{"multicluster-scheduler": "enabled"},
				Labels: map[string]string{"namespace": "scheduler"},
			},
			Status: v1.NamespaceStatus{
				Phase: v1.NamespaceActive,
			},
		}
		// 创建一个新的Namespaces
		log.Println("Creating Namespaces...")
		clientNamespaceObject, err := namespacesClient.Create(namespace)
		podIsFirstInThisNamespace := true
		if err != nil {
			panic(err)
		}
		log.Printf("Created Namespaces %s on %s\n", clientNamespaceObject.ObjectMeta.Name,
			clientNamespaceObject.ObjectMeta.CreationTimestamp)
		//创建该scheduler namespace下的pvc
		pvcObjectInThisNamespace, err := createPvc(clientService, name)
		if err != nil {
			panic(err)
		}
		return clientNamespaceObject, podIsFirstInThisNamespace, pvcObjectInThisNamespace, nil
	}
}
func recoverCreateServiceFail() {
	if r := recover(); r != nil {
		log.Println("recovered from ", r)
	}
}

// 创建调度器Service
func createSchedulerService(request *resource_allocator.CreateSchedulerPodRequest, clientService *kubernetes.Clientset,
	podNamespace *v1.Namespace) (*v1.Service, error) {
	//创建Service失败的recover错误捕获机制
	defer recoverCreateServiceFail()

	serviceExist := false
	serviceName := "scheduler-svc-" + strconv.Itoa(int(serviceNum))
	serviceClient := clientService.CoreV1().Services(podNamespace.Name)

	serviceList, err := thisClusterServiceLister.List(labels.Everything())
	if err != nil {
		log.Println(err)
	}
	//遍历本集群的切片namespaceList，如果不存在此namespace，则报错
	for _, svc := range serviceList {
		if svc.Name == serviceName {
			serviceExist = true
			break
		}
	}
	if serviceExist {
		//svcClientObject, err := serviceClient.Get(serviceName, metav1.GetOptions{})
		svcClientObject, err := thisClusterServiceLister.Services("scheduler-ns").Get(serviceName)
		if err == nil {
			log.Printf("This scheduler-svc %v is already exist.\n", serviceName)
		}
		return svcClientObject, nil
	} else {
		//如果不存在此service，则创建，然后返回
		service := &v1.Service{
			//TypeMeta: metav1.TypeMeta{Kind: serviceName},
			ObjectMeta: metav1.ObjectMeta{
				Name:   serviceName,
				Labels: map[string]string{"name": "scheduler-svc"},
			},
			Spec: v1.ServiceSpec{
				Ports: []v1.ServicePort{
					{
						Protocol: v1.ProtocolTCP,
						Port:     6060,
						TargetPort: intstr.IntOrString{
							Type:   intstr.Int,
							IntVal: 6060,
						},
						/*一个service对应个deployment部署的scheduler pod，NodePort唯一，
						供调度器控制器访问*/
						NodePort: int32(30001 + serviceNum),
					},
				},
				Selector:                 map[string]string{"app": "scheduler"},
				Type:                     "NodePort",
				ExternalIPs:              nil,
				LoadBalancerIP:           "",
				LoadBalancerSourceRanges: nil,
				ExternalName:             "",
			},
		}
		/*创建一个新的Namespaces*/
		log.Println("Creating svc...")

		svcClientObject, err := serviceClient.Create(service)
		if err != nil {
			panic(err)
		} else {
			log.Printf("%s is created successful.\n", service.Name)
		}
		return svcClientObject, nil
	}
}
func recoverCreateDeploymentFail() {
	if r := recover(); r != nil {
		log.Println("recovered from createSchedulerPod()", r)
	}
}
func int32Ptr2(i int32) *int32 { return &i }

/*调度器生成函数*/
func createSchedulerPod(request *resource_allocator.CreateSchedulerPodRequest, clientService *kubernetes.Clientset,
	podNamespace *v1.Namespace, pvcClient *v1.PersistentVolumeClaim, service *v1.Service) error {
	//golang异常处理机制，recover捕获panic
	defer recoverCreateDeploymentFail()
	// 创建调度器pod
	pod := new(v1.Pod)
	pod.TypeMeta = metav1.TypeMeta{Kind: "Pod", APIVersion: "v1"}
	pod.ObjectMeta = metav1.ObjectMeta{Name: os.Getenv("CLUSTER_ID") + "-" + "scheduler-pod-" +
		strconv.Itoa(int(schedulerPodNum)), Namespace: podNamespace.Name,
		Labels:      map[string]string{"app": "scheduler"},
		Annotations: map[string]string{"multicluster.admiralty.io/elect": ""}}
	//所有的pod的容器内挂载volume路径为/nfs/data
	volumePathInContainer := "/nfs/data"
	//调度器镜像拉取策略在调试阶段为Always
	if imagePullPolicy == "Always" {
		pod.Spec = v1.PodSpec{
			SchedulerName: schedulerName,
			RestartPolicy: v1.RestartPolicyNever,
			Volumes: []v1.Volume{
				v1.Volume{
					Name: "scheduler-pod-share-volume",
					VolumeSource: v1.VolumeSource{
						PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
							ClaimName: pvcClient.ObjectMeta.Name,
						},
					},
				},
			},
			PriorityClassName: "high-priority",
			Containers: []v1.Container{
				v1.Container{
					Name:            "scheduler-ctr",
					ImagePullPolicy: v1.PullAlways,
					Image:           request.Image,
					SecurityContext: &v1.SecurityContext{
						Capabilities: &v1.Capabilities{
							Add:  []v1.Capability{"SYS_PTRACE"},
							Drop: nil,
						},
					},
					Ports: []v1.ContainerPort{
						v1.ContainerPort{
							ContainerPort: 80,
							Protocol:      v1.ProtocolTCP,
						},
					},
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse(strconv.Itoa(int(request.Cpu)) + "m"),
							v1.ResourceMemory: resource.MustParse(strconv.Itoa(int(request.Mem)) + "Mi"),
						},
						Limits: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse(strconv.Itoa(int(request.Cpu)) + "m"),
							v1.ResourceMemory: resource.MustParse(strconv.Itoa(int(request.Mem)) + "Mi"),
						},
					},
					VolumeMounts: []v1.VolumeMount{
						v1.VolumeMount{
							Name:      "scheduler-pod-share-volume",
							MountPath: volumePathInContainer,
							//SubPath: pvcClient.ObjectMeta.Name,
						},
					},
					Env: []v1.EnvVar{
						v1.EnvVar{
							Name:  "SHARE_PATH",
							Value: volumePathInContainer,
							//ValueFrom: &v1.EnvVarSource{
							//	FieldRef: &v1.ObjectFieldSelector{
							//		FieldPath: volumePathInContainer,
							//	},
						},
						{
							Name:  "CONTROL_HOST",
							Value: os.Getenv("CONTROL_HOST"),
						},
						{
							Name:  "CONTROL_PORT",
							Value: os.Getenv("CONTROL_PORT"),
						},
						{
							Name:  "NODE_IP",
							Value: os.Getenv("NODE_IP"),
							////ValueFrom: &v1.EnvVarSource{
							//	FieldRef: &v1.ObjectFieldSelector{
							//		FieldPath:,
							//		APIVersion: "v1",
							//	},
							//},
						},
						{
							Name:  "NODE_PORT",
							Value: strconv.Itoa(int(30001 + serviceNum)),
						},
						{
							Name:  "CLUSTER_ID",
							Value: os.Getenv("CLUSTER_ID"),
						},
						{
							Name:  "REDIS0_IP",
							Value: os.Getenv("REDIS0_IP"),
						},
						{
							Name:  "REDIS1_IP",
							Value: os.Getenv("REDIS1_IP"),
						},
						{
							Name:  "REDIS2_IP",
							Value: os.Getenv("REDIS2_IP"),
						},
						{
							Name:  "REDIS0_PORT",
							Value: os.Getenv("REDIS0_PORT"),
						},
						{
							Name:  "REDIS1_PORT",
							Value: os.Getenv("REDIS1_PORT"),
						},
						{
							Name:  "REDIS2_PORT",
							Value: os.Getenv("REDIS2_PORT"),
						},
						{
							Name:  "PROMETHEUS_IP",
							Value: os.Getenv("PROMETHEUS_IP"),
						},
						{
							Name:  "PROMETHEUS_PORT",
							Value: os.Getenv("PROMETHEUS_PORT"),
						},
					},
				},
			},
		}
	} else {
		pod.Spec = v1.PodSpec{
			SchedulerName: schedulerName,
			RestartPolicy: v1.RestartPolicyNever,
			Volumes: []v1.Volume{
				v1.Volume{
					Name: "scheduler-pod-share-volume",
					VolumeSource: v1.VolumeSource{
						PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
							ClaimName: pvcClient.ObjectMeta.Name,
						},
					},
				},
			},
			PriorityClassName: "high-priority",
			Containers: []v1.Container{
				v1.Container{
					Name:            "scheduler-ctr",
					ImagePullPolicy: v1.PullIfNotPresent,
					Image:           request.Image,
					SecurityContext: &v1.SecurityContext{
						Capabilities: &v1.Capabilities{
							Add:  []v1.Capability{"SYS_PTRACE"},
							Drop: nil,
						},
					},
					Ports: []v1.ContainerPort{
						v1.ContainerPort{
							ContainerPort: 80,
							Protocol:      v1.ProtocolTCP,
						},
					},
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse(strconv.Itoa(int(request.Cpu)) + "m"),
							v1.ResourceMemory: resource.MustParse(strconv.Itoa(int(request.Mem)) + "Mi"),
						},
						Limits: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse(strconv.Itoa(int(request.Cpu)) + "m"),
							v1.ResourceMemory: resource.MustParse(strconv.Itoa(int(request.Mem)) + "Mi"),
						},
					},
					VolumeMounts: []v1.VolumeMount{
						v1.VolumeMount{
							Name:      "scheduler-pod-share-volume",
							MountPath: volumePathInContainer,
							//SubPath: pvcClient.ObjectMeta.Name,
						},
					},
					Env: []v1.EnvVar{
						v1.EnvVar{
							Name:  "SHARE_PATH",
							Value: volumePathInContainer,
							//ValueFrom: &v1.EnvVarSource{
							//	FieldRef: &v1.ObjectFieldSelector{
							//		FieldPath: volumePathInContainer,
							//	},
						},
						{
							Name:  "CONTROL_HOST",
							Value: os.Getenv("CONTROL_HOST"),
						},
						{
							Name:  "CONTROL_PORT",
							Value: os.Getenv("CONTROL_PORT"),
						},
						{
							Name:  "NODE_IP",
							Value: os.Getenv("NODE_IP"),
							////ValueFrom: &v1.EnvVarSource{
							//	FieldRef: &v1.ObjectFieldSelector{
							//		FieldPath:,
							//		APIVersion: "v1",
							//	},
							//},
						},
						{
							Name:  "NODE_PORT",
							Value: strconv.Itoa(int(30001 + serviceNum)),
						},
						{
							Name:  "CLUSTER_ID",
							Value: os.Getenv("CLUSTER_ID"),
						},
						{
							Name:  "REDIS0_IP",
							Value: os.Getenv("REDIS0_IP"),
						},
						{
							Name:  "REDIS1_IP",
							Value: os.Getenv("REDIS1_IP"),
						},
						{
							Name:  "REDIS2_IP",
							Value: os.Getenv("REDIS2_IP"),
						},
						{
							Name:  "REDIS0_PORT",
							Value: os.Getenv("REDIS0_PORT"),
						},
						{
							Name:  "REDIS1_PORT",
							Value: os.Getenv("REDIS1_PORT"),
						},
						{
							Name:  "REDIS2_PORT",
							Value: os.Getenv("REDIS2_PORT"),
						},
						{
							Name:  "PROMETHEUS_IP",
							Value: os.Getenv("PROMETHEUS_IP"),
						},
						{
							Name:  "PROMETHEUS_PORT",
							Value: os.Getenv("PROMETHEUS_PORT"),
						},
					},
				},
			},
		}
	}
	_, err := clientService.CoreV1().Pods(podNamespace.Name).Create(pod)
	//_, err := clientService.CoreV1().Services(podNamespace.Name).Create(*v1.Service)
	if err != nil {
		panic(err.Error())
	}
	schedulerPodNum++
	serviceNum++
	//namespace := podNamespace.Name
	pods, err := clientService.CoreV1().Pods(podNamespace.Name).List(metav1.ListOptions{})
	if err != nil {
		panic(err)
	}
	log.Printf("There are %d pods in scheduler-ns namespaces %s\n", len(pods.Items), podNamespace.Name)
	return nil
	////所有的pod的容器内挂载volume路径为/nfsdata/scheduler
	//volumePathInContainer := "/nfs/data"
	////var (
	//	deployment := &appsv1.Deployment{
	//		ObjectMeta: metav1.ObjectMeta{
	//			Name:   "scheduler-deploy-" + strconv.Itoa(int(schedulerPodNum)),
	//			Labels: map[string]string{"app": "scheduler",},
	//		},
	//		Spec: appsv1.DeploymentSpec{
	//			Replicas: int32Ptr2(1),
	//			Selector: &metav1.LabelSelector{
	//				MatchLabels: map[string]string{"app": "scheduler"},
	//			},
	//			Template: v1.PodTemplateSpec{
	//				ObjectMeta: metav1.ObjectMeta{
	//					Labels:      map[string]string{"app": "scheduler",},
	//					Name:        "schedulerPod-" + strconv.Itoa(int(schedulerPodNum)),
	//					Namespace:   podNamespace.Name,
	//					Annotations: map[string]string{"multicluster.admiralty.io/elect": ""},
	//				},
	//				Spec: v1.PodSpec{
	//					SchedulerName: schedulerName,
	//					RestartPolicy: v1.RestartPolicyAlways,
	//					Volumes: []v1.Volume{
	//						v1.Volume{
	//							Name: "scheduler-pod-share-volume",
	//							VolumeSource: v1.VolumeSource{
	//								PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
	//									ClaimName: pvcClient.ObjectMeta.Name,
	//								},
	//							},
	//						},
	//					},
	//					PriorityClassName: "high-priority",
	//					Containers: []v1.Container{
	//						{
	//							Name:            "scheduler-ctr",
	//							ImagePullPolicy: v1.PullAlways,
	//							Image:           request.Image,
	//							//SecurityContext: &v1.SecurityContext{
	//							//	Capabilities: &v1.Capabilities{
	//							//		Add:  []v1.Capability{"SYS_PTRACE"},
	//							//		Drop: nil,
	//							//	},
	//							//},
	//							Resources: v1.ResourceRequirements{
	//								Requests: v1.ResourceList{
	//									v1.ResourceCPU:    resource.MustParse(strconv.Itoa(int(request.Cpu)) + "m"),
	//									v1.ResourceMemory: resource.MustParse(strconv.Itoa(int(request.Mem)) + "Mi"),
	//								},
	//								Limits: v1.ResourceList{
	//									v1.ResourceCPU:    resource.MustParse(strconv.Itoa(int(request.Cpu)) + "m"),
	//									v1.ResourceMemory: resource.MustParse(strconv.Itoa(int(request.Mem)) + "Mi"),
	//								},
	//							},
	//							VolumeMounts: []v1.VolumeMount{
	//								v1.VolumeMount{
	//									Name:      "scheduler-pod-share-volume",
	//									MountPath: volumePathInContainer,
	//									//SubPath: pvcClient.ObjectMeta.Name,
	//								},
	//							},
	//							Env: []v1.EnvVar{
	//								v1.EnvVar{
	//									Name:  "SHARE_PATH",
	//									Value: volumePathInContainer,
	//									//ValueFrom: &v1.EnvVarSource{
	//									//	FieldRef: &v1.ObjectFieldSelector{
	//									//		FieldPath: volumePathInContainer,
	//									//	},
	//								},
	//								{
	//									Name:  "CONTROL_HOST",
	//									Value: os.Getenv("CONTROL_HOST"),
	//								},
	//								{
	//									Name:  "CONTROL_PORT",
	//									Value: os.Getenv("CONTROL_PORT"),
	//								},
	//								{
	//									Name: "NODE_IP",
	//									Value: os.Getenv("NODE_IP"),
	//									//ValueFrom: &v1.EnvVarSource{
	//									//	FieldRef: &v1.ObjectFieldSelector{
	//									//		FieldPath:,
	//									//		APIVersion: "v1",
	//									//	},
	//									//},
	//								},
	//								{
	//									Name:  "NODE_PORT",
	//									Value: strconv.Itoa(int(30000 + serviceNum)),
	//								},
	//								{
	//									Name:  "CLUSTER_ID",
	//									Value: os.Getenv("CLUSTER_ID"),
	//								},
	//							},
	//							Ports: []v1.ContainerPort{
	//								{
	//									Name:          "http",
	//									Protocol:      v1.ProtocolTCP,
	//									ContainerPort: 80,
	//								},
	//							},
	//						},
	//					},
	//				},
	//			},
	//		},
	//	}
	////)
	////result, err := clientService.AppsV1beta1().Deployments(podNamespace.Name).Create(deployment)
	//result, err := clientService.AppsV1().Deployments(podNamespace.Name).Create(deployment)
	//if err != nil {
	//	panic(err.Error())
	//}
	//log.Printf("Created deployment %q.\n", result.GetObjectMeta().GetName())
	//
	//return nil
}

// 获取集群剩余资源
func getResidualResource(request *resource_allocator.ResourceRequest) *resource_allocator.ResourceAllocateInfo {

	//get residual resource of k8s cluster
	allocateAlgorithmResult := resourceAllocateAlgorithm(request)
	cpuNum := allocateAlgorithmResult.MilliCpu
	memNum := allocateAlgorithmResult.Memory

	residualResource := resource_allocator.ResourceAllocateInfo{
		SchedulerId: request.GetSchedulerId(),
		CurrentRequest: &resource_allocator.ResourceAllocateInfoResourceDemand{
			Cpu: int64(cpuNum),
			Mem: int64(memNum)},
		CurrentRequestStatus: true}
	//fmt.Printf("cpu = %dm, mem = %dMi in getResidualResource.\n",cpuNum,memNum)

	return &residualResource
}

func recoverResourceAllocateAlgorithmFail() {
	if r := recover(); r != nil {
		log.Println("recovered from ", r)
	}
}

// 自适应资源分配算法
func adapResourceAllocateAlgorithm(task *resource_allocator.CreateTaskPodRequest) (cpuNum uint64, memNum uint64) {

	//Get resource quota for the current task pod through for loop.
	cpuResidualResourceValue := uint64(0)
	memResidualResourceValue := uint64(0)
	nodeResidualResourceMaxCpu := uint64(0)
	nodeResidualResourceMaxMem := uint64(0)
	//Obtain the map of remaining resources of each node
	ResidualMap := k8sResource.GetK8sEachNodeResource(podLister, nodeLister, nodeResidualMap)
	//Obtain the largest remaining resources in the cluster nodes
	for _, val := range ResidualMap {
		cpuResidualResourceValue += val.MilliCpu
		memResidualResourceValue += val.Memory
		//Ensure that the cpu and memory resources belong to the same node.
		if (val.MilliCpu > nodeResidualResourceMaxCpu) && (val.MilliCpu != 0) {
			nodeResidualResourceMaxCpu = val.MilliCpu
			nodeResidualResourceMaxMem = val.Memory
		}
		//if (val.Memory > nodeResidualResourceMaxMem) && (val.Memory != 0) {
		//  nodeResidualResourceMaxMem = val.Memory
		//}
	}
	log.Printf("cpuResidualResourceValue: %v,memResidualResourceValue: %v\n",
		cpuResidualResourceValue, memResidualResourceValue)

	if (task.Cpu != 0) && (task.Mem != 0) {
		//(1)If the maximum remaining resources of a cluster node are greater than the resources required by the current
		//     task, a task container can be generated*/
		if (uint64(task.Cpu) <= cpuResidualResourceValue) &&
			(uint64(task.Mem) <= memResidualResourceValue) {
			if (uint64(task.Cpu) < nodeResidualResourceMaxCpu) && (uint64(task.Mem) < nodeResidualResourceMaxMem) {
				//Create this task POD if the cluster resources are sufficient.
				cpuNum = uint64(task.Cpu)
				memNum = uint64(task.Mem)
				log.Printf("allocation pod's cpuNum: %dm, allocation pod's memNum: %dMi.\n", cpuNum, memNum)
				//return cpuNum, memNum
			} else {
				if (uint64(task.Cpu) >= nodeResidualResourceMaxCpu) && (uint64(task.Mem) < nodeResidualResourceMaxMem) {
					cpuNum = nodeResidualResourceMaxCpu * 8 / 10
					memNum = uint64(task.Mem)
					log.Printf("allocation pod's cpuNum: %dm, allocation pod's memNum: %dMi.\n", cpuNum, memNum)
					//return cpuNum, memNum
				} else {
					if (uint64(task.Cpu) < nodeResidualResourceMaxCpu) && (uint64(task.Mem) >= nodeResidualResourceMaxMem) {
						cpuNum = uint64(task.Cpu)
						memNum = nodeResidualResourceMaxMem * 8 / 10
						log.Printf("allocation pod's cpuNum: %dm, allocation pod's memNum: %dMi.\n", cpuNum, memNum)
						//return cpuNum, memNum
					} else {
						cpuNum = nodeResidualResourceMaxCpu * 8 / 10
						memNum = nodeResidualResourceMaxMem * 8 / 10
						log.Printf("allocation pod's cpuNum: %dm, allocation pod's memNum: %dMi.\n", cpuNum, memNum)
						//return cpuNum, memNum
					}
				}
			}
		} else {
			//(2) The CPU resources of the task to be created are insufficient.
			//     Compress the CPU resources of tasks in proportion.
			if (uint64(task.Cpu) > cpuResidualResourceValue) &&
				(uint64(task.Mem) < memResidualResourceValue) {
				//compress the Cpu's requests resource
				cpuNumTemp := uint64(task.Cpu) * cpuResidualResourceValue / uint64(task.Cpu)

				if (cpuNumTemp < nodeResidualResourceMaxCpu) && (uint64(task.Mem) < nodeResidualResourceMaxMem) {
					//Create this task POD if the cluster resources are sufficient.
					cpuNum = cpuNumTemp
					memNum = uint64(task.Mem)
					log.Printf("allocation pod's cpuNum: %dm, allocation pod's memNum: %dMi.\n", cpuNum, memNum)
					//return cpuNum, memNum
				} else {
					if (cpuNumTemp >= nodeResidualResourceMaxCpu) && (uint64(task.Mem) < nodeResidualResourceMaxMem) {
						cpuNum = nodeResidualResourceMaxCpu * 8 / 10
						memNum = uint64(task.Mem)
						log.Printf("allocation pod's cpuNum: %dm, allocation pod's memNum: %dMi.\n", cpuNum, memNum)
						//return cpuNum, memNum
					} else {
						if (cpuNumTemp < nodeResidualResourceMaxCpu) && (uint64(task.Mem) >= nodeResidualResourceMaxMem) {
							cpuNum = cpuNumTemp
							memNum = nodeResidualResourceMaxMem * 8 / 10
							log.Printf("allocation pod's cpuNum: %dm, allocation pod's memNum: %dMi.\n", cpuNum, memNum)
							//return cpuNum, memNum
						} else {
							cpuNum = nodeResidualResourceMaxCpu * 8 / 10
							memNum = nodeResidualResourceMaxMem * 8 / 10
							log.Printf("allocation pod's cpuNum: %dm, allocation pod's memNum: %dMi.\n", cpuNum, memNum)
							//return cpuNum, memNum
						}
					}
				}
			} else {
				//(3)The MEM resources of the task to be created are insufficient.
				//Compress the MEM resources of tasks in proportion.
				if (uint64(task.Cpu) < cpuResidualResourceValue) &&
					(uint64(task.Mem) > memResidualResourceValue) {
					//compress the memory's requests resource
					memNumTemp := uint64(task.Mem) * memResidualResourceValue / uint64(task.Mem)
					if (uint64(task.Cpu) < nodeResidualResourceMaxCpu) && (memNumTemp < nodeResidualResourceMaxMem) {
						//Create this task POD if the cluster resources are sufficient.
						cpuNum = uint64(task.Cpu)
						memNum = memNumTemp
						log.Printf("allocation pod's cpuNum: %dm, allocation pod's memNum: %dMi.\n", cpuNum, memNum)
						//return cpuNum, memNum
					} else {
						if (uint64(task.Cpu) >= nodeResidualResourceMaxCpu) && (memNumTemp < nodeResidualResourceMaxMem) {
							cpuNum = nodeResidualResourceMaxCpu * 8 / 10
							memNum = memNumTemp
							log.Printf("allocation pod's cpuNum: %dm, allocation pod's memNum: %dMi.\n", cpuNum, memNum)
							//return cpuNum, memNum
						} else {
							if (uint64(task.Cpu) < nodeResidualResourceMaxCpu) && (memNumTemp >= nodeResidualResourceMaxMem) {
								cpuNum = uint64(task.Cpu)
								memNum = nodeResidualResourceMaxMem * 8 / 10
								log.Printf("allocation pod's cpuNum: %dm, allocation pod's memNum: %dMi.\n", cpuNum, memNum)
								//return cpuNum, memNum
							} else {
								cpuNum = nodeResidualResourceMaxCpu * 8 / 10
								memNum = nodeResidualResourceMaxMem * 8 / 10
								log.Printf("allocation pod's cpuNum: %dm, allocation pod's memNum: %dMi.\n", cpuNum, memNum)
								//return cpuNum, memNum
							}
						}
					}
				} else {
					//(4) Both resources of the task to be created are insufficient.
					//Compress the CPU and MEM resources of tasks in proportion.
					cpuNumTemp := uint64(task.Cpu) * cpuResidualResourceValue / uint64(task.Cpu)
					memNumTemp := uint64(task.Mem) * memResidualResourceValue / uint64(task.Mem)
					cpuNum = cpuNumTemp
					memNum = memNumTemp
					log.Printf("allocation pod's cpuNum: %dm, allocation pod's memNum: %dMi.\n", cpuNum, memNum)
				}
			}
		}
	} else {
		//failed to acquire resource data from redis.
		if uint64(task.Cpu) < nodeResidualResourceMaxCpu && uint64(task.Mem) < nodeResidualResourceMaxMem {
			cpuNum = uint64(task.Cpu)
			memNum = uint64(task.Mem)
			log.Printf("获取redis总的资源总任务资源为0，没有链接上，适当非配资源为requests,allocation cpuNum: %dm, "+
				"allocation memNum: %dMi.\n", cpuNum, memNum)
			//return cpuNum, memNum
		} else {
			if uint64(task.Cpu) >= nodeResidualResourceMaxCpu && uint64(task.Mem) < nodeResidualResourceMaxMem {
				cpuNum = nodeResidualResourceMaxCpu * 8 / 10
				memNum = uint64(task.Mem)
				log.Printf("获取redis总的资源总任务资源为0，没有链接上，适当非配资源为requests,allocation cpuNum: %dm, "+
					"allocation memNum: %dMi.\n", cpuNum, memNum)
				//return cpuNum, memNum
			} else {
				if uint64(task.Cpu) < nodeResidualResourceMaxCpu && uint64(task.Mem) >= nodeResidualResourceMaxMem {
					cpuNum = uint64(task.Cpu)
					memNum = nodeResidualResourceMaxMem * 8 / 10
					log.Printf("获取redis总的资源总任务资源为0，没有链接上，适当非配资源为requests,allocation cpuNum: %dm, "+
						"allocation memNum: %dMi.\n", cpuNum, memNum)
					//return cpuNum, memNum
				} else {
					cpuNum = nodeResidualResourceMaxCpu * 8 / 10
					memNum = nodeResidualResourceMaxMem * 8 / 10
					log.Printf("获取redis总的资源总任务资源为0，没有链接上，适当非配资源为requests,allocation cpuNum: %dm, "+
						"allocation memNum: %dMi.\n", cpuNum, memNum)
					//return cpuNum, memNum
				}
			}
		}
	}
	//Check whether the assigned value meets the resource limit of the task container
	//stress -m 100 (require ) In experiments, the container requires 20 Mi more memory
	//than allocated memory by stress.
	//CPU resources are compressible and unlimited.
	//To ensure that the container works properly, we set the minimum value.
	//TODO
	//if cpuNum >= task.MinCpu && memNum >= (task.MinMem+20) {
	//	break
	//}
	return cpuNum, memNum
}

// 资源分配算法, 一批任务资源需求会存储的ETCD，返回给调度器可用资源，调度器根据可用资源进行工作流调度
// TODO可以改成资源足够时返回请求的资源，资源不足时返回当前的所有可用资源
func resourceAllocateAlgorithm(request *resource_allocator.ResourceRequest) resourceAllocation {
	defer recoverResourceAllocateAlgorithmFail()
	var allTasksRequestCpuNum uint64
	var allTasksRequestMemNum uint64
	var cpuNum uint64
	var memNum uint64
	//var mu sync.Mutex
	//schedulerId, err:= strconv.Atoi(request.RequestId[:8])
	//get scheduler Id in resource request data structure
	currentRequestCpuNum := uint64(request.CurrentRequest.GetCpu())
	currentRequestMemNum := uint64(request.CurrentRequest.GetMem())
	log.Println("------------------------------------------")
	log.Printf("currentRequestCpuNum = %dm, currentRequestMemNum = %dMi.\n", currentRequestCpuNum,
		currentRequestMemNum)

	//mu.Lock()
	allTasksRequestCpuNum, allTasksRequestMemNum = readAndWriteEtcd(request)
	//mu.Unlock()
	getK8sResource := k8sResource.GetK8sApiResource(podLister, nodeLister, masterIp)
	log.Printf("Cluster's residual resource: cpu = %dm, mem = %dMi .\n", getK8sResource.MilliCpu, getK8sResource.Memory)
	//若customization=true且cost_grade=false，则请求的资源都满足,调度器要多少给多少
	if request.Customization == true && request.CostGrade == false {
		log.Printf("customization=true&&cost_grade=false,Cpunum= %dm,Memnum = %dMi\n", currentRequestCpuNum, currentRequestMemNum)
		log.Println("进入定制化工作流.")
		if getK8sResource.MilliCpu == 0 {
			log.Printf("If residual resources are zero, allocating Cpunum= %dm,allocating Memnum = %dMi\n", 0, 0)
			return resourceAllocation{MilliCpu: 0, Memory: 0}
		} else {
			log.Printf("Residual resources are sufficient, allocating Cpunum= %dm,"+
				"allocating Memnum = %dMi\n", currentRequestCpuNum, currentRequestMemNum)
			return resourceAllocation{MilliCpu: uint64(currentRequestCpuNum), Memory: uint64(currentRequestMemNum)}
		}
	} else //否则，按照之前的策略分配
	{
		if getK8sResource.MilliCpu != 0 && getK8sResource.Memory != 0 {
			podList, err := podLister.List(labels.Everything())
			if err != nil {
				log.Println(err)
			}
			if len(podList) <= 500 {
				log.Printf("本集群len(podList)=%d,小于500,进入非定制化工作流处理过程", len(podList))
				log.Println("No customization.")
				//当ETCD所有当前保活调度器的资源需求不为0时
				if (allTasksRequestCpuNum != 0) && (allTasksRequestMemNum != 0) {
					log.Printf("All scheduler request resource Cpunum = %dm,Memnum = %dMi\n", allTasksRequestCpuNum, allTasksRequestMemNum)
					otherCpuNum := currentRequestCpuNum * getK8sResource.MilliCpu / allTasksRequestCpuNum
					otherMemNum := currentRequestMemNum * getK8sResource.Memory / allTasksRequestMemNum
					if (getK8sResource.MilliCpu > allTasksRequestCpuNum) && (getK8sResource.Memory > allTasksRequestMemNum) {
						log.Printf("allocation cpuNum: %dm, allocation memNum: %dMi.\n", currentRequestCpuNum, currentRequestMemNum)
						return resourceAllocation{MilliCpu: uint64(currentRequestCpuNum), Memory: uint64(currentRequestMemNum)}
					} else {
						if (getK8sResource.MilliCpu < allTasksRequestCpuNum) && (getK8sResource.Memory > allTasksRequestMemNum) {
							log.Printf("allocation cpuNum: %dm, allocation memNum: %dMi.\n", otherCpuNum, currentRequestMemNum)
							//if otherCpuNum < 1000 {
							//	otherCpuNum = 1000
							//	log.Println("If otherCpuNum < 1000, otherCpuNum = 1000.")
							//}
							return resourceAllocation{MilliCpu: uint64(otherCpuNum), Memory: uint64(currentRequestMemNum)}
						} else {
							if (getK8sResource.MilliCpu > allTasksRequestCpuNum) && (getK8sResource.Memory < allTasksRequestMemNum) {
								log.Printf("allocation cpuNum: %dm, allocation memNum: %dMi.\n", currentRequestCpuNum, otherMemNum)
								return resourceAllocation{MilliCpu: currentRequestCpuNum, Memory: otherMemNum}
							} else {
								log.Printf("allocation cpuNum: %dm, allocation memNum: %dMi.\n", otherCpuNum, otherMemNum)
								//if otherCpuNum < 1000 {
								//	otherCpuNum = 1000
								//	log.Println("If otherCpuNum < 1000, otherCpuNum = 1000.")
								//}
								return resourceAllocation{MilliCpu: otherCpuNum, Memory: otherMemNum}
							}
						}
					}
				} else {
					//getK8sCluster1Resource := k8sResource.GetK8sApiResource(cluster1PodLister,cluster1NodeLister)
					//    log.Println(getK8sCluster1Resource)
					//getK8sCluster2Resource := k8sResource.GetK8sApiResource(cluster2PodLister,cluster2NodeLister)
					//    log.Println(getK8sCluster2Resource)
					//getK8sCluster3Resource := k8sResource.GetK8sApiResource(cluster3PodLister,cluster3NodeLister)
					//    log.Println(getK8sCluster3Resource)
					//getK8sResource.MilliCpu = getK8sCluster1Resource.MilliCpu + getK8sCluster2Resource.MilliCpu + getK8sCluster3Resource.MilliCpu
					//getK8sResource.Memory = getK8sCluster1Resource.Memory + getK8sCluster2Resource.Memory + getK8sCluster3Resource.Memory
					//getK8sResource.EphemeralStorage = getK8sCluster1Resource.EphemeralStorage + getK8sCluster2Resource.EphemeralStorage + getK8sCluster3Resource.EphemeralStorage
					/*当连不上etcd，分配剩余资源的一百分之一*/
					cpuNum = getK8sResource.MilliCpu / 20
					memNum = getK8sResource.Memory / 20
					log.Printf("获取etcd总的资源总任务资源为0，没有链接上，适当分配二十分之一剩余资源,allocation cpuNum: %dm, allocation memNum: %dMi.\n", cpuNum, memNum)
					//if cpuNum < 1000 {
					//	cpuNum = 1000
					//	log.Println("If otherCpuNum < 1000, otherCpuNum = 1000.")
					//}
					return resourceAllocation{MilliCpu: cpuNum, Memory: memNum}
				}
			} else {
				log.Println("the pod's number in this cluster is larger than 1000. No allocation resource.")
				return resourceAllocation{MilliCpu: 0, Memory: 0}
			}
		} else {
			log.Println("Cluster residual resource are zero. No allocation resource.")
			return resourceAllocation{MilliCpu: 0, Memory: 0}
		}
	}
}

func recoverReadWriteEtcdFail() {
	if r := recover(); r != nil {
		log.Println("recovered from ", r)
	}
}
func readAndWriteEtcd(request *resource_allocator.ResourceRequest) (globalRequestResourceCpu, globalRequestResourceMem uint64) {
	defer recoverReadWriteEtcdFail()
	var allTasksRequestCpu uint64
	var allTasksRequestMem uint64
	var unMarshalData requestResourceConfig
	//获取clinetv3
	cli := getClinetv3()
	if cli == nil {
		log.Println("get *clientv3.Client failed.")
	}
	defer cli.Close()

	//增加调度器ID号前缀scheduler/
	schedulerId := "scheduler/" + request.SchedulerId[0:22]
	currentAllTasksRequestCpuNum := uint64(request.AllTasksRequest.GetCpu())
	currentAllTasksRequestMemNum := uint64(request.AllTasksRequest.GetMem())
	currentAllTasksResourceRequestData := requestResourceConfig{Timestamp: request.TimeStamp, AliveStatus: true,
		ResourceDemand: resourceAllocation{Memory: currentAllTasksRequestMemNum, MilliCpu: currentAllTasksRequestCpuNum}}
	//结构体序列化为字符串
	log.Println(currentAllTasksResourceRequestData)
	data, err := json.Marshal(currentAllTasksResourceRequestData)
	if err != nil {
		log.Println("json Marshal is err: ", err)
		return
	}
	//设置0.1秒超时，访问etcd有超时控制
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	//写入etcd key-value
	_, err = cli.Put(ctx, schedulerId, string(data))
	//操作完毕，取消etcd
	cancel()
	if err != nil {
		log.Println("put currentAllTasksResourceRequestData failed, err:", err)
		return
	}
	//读取etcd过程.取值，设置超时为1秒
	ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
	// clientv3.WithPrefix() 这个option的意思就是以前缀获取,就是只要有这个前缀的key都返回.
	resp, err := cli.Get(ctx, "scheduler/", clientv3.WithPrefix())
	cancel()
	if err != nil {
		log.Println("get currentAllTasksResourceRequestData map failed, err:", err)
		return
	}
	//遍历读取etcd的key-value
	log.Println("-------------------------------------------")
	for _, ev := range resp.Kvs {
		log.Printf("%s : %s\n", ev.Key, ev.Value)
		//反序列化得到结构体unMarshalData
		err := json.Unmarshal(ev.Value, &unMarshalData)
		if err != nil {
			log.Println("json Marshal is err: ", err)
			return
		}
		//获取保活信号的scheduler的资源总需求
		if unMarshalData.AliveStatus == true {
			allTasksRequestCpu += unMarshalData.ResourceDemand.MilliCpu
			allTasksRequestMem += unMarshalData.ResourceDemand.Memory
			//log.Println(unMarshalData.AliveStatus)
			//log.Println(unMarshalData.Timestamp)
			//log.Println(unMarshalData.ResourceDemand)
		}
	}
	log.Println("-------------------------------------------")
	return allTasksRequestCpu, allTasksRequestMem
}

func recoverConnectEtcdFail() {
	if r := recover(); r != nil {
		log.Println("recovered from ", r)
	}
}
func getClinetv3() *clientv3.Client {
	//获取etcd主机端口环境变量
	defer recoverConnectEtcdFail()
	//获取etcd主机端口环境变量
	endpoints := getEtcdEnv()
	//封装证书访问
	cert, err := tls.LoadX509KeyPair(etcdCert, etcdCertKey)
	if err != nil {
		return nil
	}
	caData, err := ioutil.ReadFile(etcdCa)
	if err != nil {
		return nil
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caData)
	//创建config
	_tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
	}
	cfg := clientv3.Config{
		Endpoints: []string{endpoints},
		TLS:       _tlsConfig,
	}
	//cli, err := clientv3.New(clientv3.Config{
	//	Endpoints:  []string{ endpoints },
	//	DialTimeout: dialTimeout,
	//})
	//create clientv3
	client, err := clientv3.New(cfg)
	if err != nil {
		log.Println("connect failed, err:", err)
		return nil
	}
	log.Println("Connect etcd successfully.")
	return client
}
func connectController(waiter *sync.WaitGroup) {
	defer waiter.Done()
	var i uint32 = 0
	SchedulerControlIP := os.Getenv("CONTROL_HOST") + ":" + os.Getenv("CONTROL_PORT")
	//获取当前cluster.id,PodIP+Port
	//clusterId := os.Getenv("CLUSTER_ID")
	nodePort := os.Getenv("NODE_IP") + ":" + "30000"
	limit := make(chan string, 1)
	for {
		limit <- "s"
		time.AfterFunc(5*time.Second, func() {
			//log.Println("Conecting to ",SchedulerControlIP)
			//1.Dial连接
			conn, err := grpc.Dial(SchedulerControlIP, grpc.WithInsecure())
			if err != nil {
				panic(err.Error())
			}
			//defer conn.Close() //主程序执行完毕defer语句执行
			// 通过gRPC发送给调度器控制器nodePort
			clientRegisterRA := scheduler_controller.NewSchedulerControllerClient(conn)

			/*获取环境变量cluster_id*/
			ReplyInfo := &scheduler_controller.RegisterRARequest{ClusterId: clusterId, Ipv4: nodePort}
			//log.Println("Sending ",ReplyInfo)
			//远程访问此任务请求
			clientRegisterRAResponse, err := clientRegisterRA.RegisterResourceAllocator(context.Background(), ReplyInfo)
			//log.Println("Reply from secheduling-controller:",clientRegisterRAResponse)
			/*核对前后两次的boot_id是否一致，一致controllerBackOffFlag为false.*/
			if clientRegisterRAResponse != nil {
				bootIdArray[i] = clientRegisterRAResponse.BootId
				i++
				if i == 1 || bootIdArray[0] == bootIdArray[1] {
					controllerBackOffFlag = false
					//bootIdArray[0] = bootIdArray[1]
					i = 1
				} else {
					controllerBackOffFlag = true
					bootIdArray[i-2] = bootIdArray[i-1]
					i = 1
				}
				//log.Println(controllerBackOffFlag)
			}
			conn.Close()
			<-limit
		})
	}
}

// 建立grpc服务Server
func grpcServer(waiter *sync.WaitGroup) {
	defer waiter.Done()
	//建立grpc服务器
	server := grpc.NewServer()
	log.Println("Build gRPC Server.")
	//注册资源请求服务
	//resource_allocator.RegisterResourceRequestServiceServer(server, new(ResourceServiceImpl))
	resource_allocator.RegisterResourceRequestServiceServer(server, new(ResourceServiceImpl))
	log.Println("Register resource request protobuf service.")
	//监听本机8000port
	lis, err := net.Listen("tcp", ":6060")
	log.Println("Listening local port 6060.")
	if err != nil {
		panic(err.Error())
	}
	server.Serve(lis)

}

// 获取etcd的环境变量
func getEtcdEnv() (etcdEnv string) {
	etcdEnv = os.Getenv("ETCD_HOST") + ":" + os.Getenv("ETCD_PORT")
	log.Println(etcdEnv)
	return etcdEnv
}

func createSchedulerNsInCurrentCluster(clientService *kubernetes.Clientset) error {
	//name := request.
	namespaceExist := false
	name := "scheduler-ns"
	namespacesClient := clientService.CoreV1().Namespaces()
	namespaceList, err := thisClusterNamespaceLister.List(labels.Everything())
	if err != nil {
		log.Println(err)
	}
	//遍历本集群的切片namespaceList，如果不存在此namespace，则创建
	for _, ns := range namespaceList {
		if ns.Name == name {
			namespaceExist = true
			break
		}
	}
	if namespaceExist {
		_, err := namespacesClient.Get(name, metav1.GetOptions{})
		if err == nil {
			log.Printf("This namespace %v is already exist.\n", name)
		}
		return nil
	} else {
		//如果不存在此namespace，则创建
		//设置label，供multicluster-scheduler调度使用
		namespace := &v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:   name,
				Labels: map[string]string{"multicluster-scheduler": "enabled"},
			},
			Status: v1.NamespaceStatus{
				Phase: v1.NamespaceActive,
			},
		}
		// 创建一个新的Namespaces
		log.Println("Creating Namespace scheduler ns.")
		clientNamespaceObject, err := namespacesClient.Create(namespace)
		if err != nil {
			panic(err)
		}
		log.Printf("Created Namespaces %s on %s\n", clientNamespaceObject.ObjectMeta.Name,
			clientNamespaceObject.ObjectMeta.CreationTimestamp)
		return nil
	}
}
func recoverInitNodeResidualResourceMap() {
	if r := recover(); r != nil {
		log.Println("recovered from ", r)
	}
}
func initNodeResidualResourceMap(resourceMap k8sResource.ResidualResourceMap, clusterMasterIp string) k8sResource.ResidualResourceMap {
	defer recoverInitNodeResidualResourceMap()
	splitName := strings.Split(clusterMasterIp, ".")
	nodeIpFourthField, err := strconv.Atoi(splitName[len(splitName)-1])
	if err != nil {
		panic(err)
	}
	nodeNum, err := strconv.Atoi(os.Getenv("NODE_NUM"))
	if err != nil {
		panic(err)
	}
	nodeIpThirdField, err := strconv.Atoi(splitName[2])
	if err != nil {
		panic(err)
	}
	nodeIpPrefix := splitName[0] + "." + splitName[1] + "." + splitName[2] + "."
	for i := 1; i <= nodeNum; i++ {
		if (nodeIpFourthField + i) < 256 {
			nodeResidualResourceKey := nodeIpPrefix + strconv.Itoa(nodeIpFourthField+i)
			resourceMap[nodeResidualResourceKey] = k8sResource.NodeResidualResource{0, 0}
		} else {
			nodeIpFourthField = nodeIpFourthField + i - 256
			nodeIpThirdField = nodeIpThirdField + 1
			nodeIpPrefix = splitName[0] + "." + splitName[1] + "." + strconv.Itoa(nodeIpThirdField) + "."
			nodeResidualResourceKey := nodeIpPrefix + strconv.Itoa(nodeIpFourthField+i)
			resourceMap[nodeResidualResourceKey] = k8sResource.NodeResidualResource{0, 0}
		}
	}
	log.Println(resourceMap)
	return resourceMap
}

func main() {
	/*由于考虑当前系统的部署：每个集群Service NodePort方式部署一个RA pod,通过跨集群pod通信机制连接主集群的Service
	方式部署的调度器控制器，主集群的IP地址需要env方式注入当前RA pod的Configmap,RA Pod通过获取环境变量方式得到
	主集群的Master的IP地址，此IP地址和端口能够通过Service NodePort方式远程访问注册到主集群的调度器控制器pod。
	调度器pod的生成：调度器由每个集群的RA pod生成，考虑到由调度器控制，由各个集群RA pod生成的调度器pod，需要
	跨集群与主集群的调度器控制器通信，所以各个调度器生成代码要包含由Configmap方式注入env到调度器pod，调度器pod运行
	后可以通过获取env得到。
	任务生成部分：工作流任务依赖于各个集群的调度器pod，每个调度器所请求的工作流必然分配到各自的集群内，该工作流所有
	任务也是如此。同一工作流的前后任务的数据传递依赖于env方式，由调度器请求的proto文件定义。
	*/
	//存放日志到指定文件
	logFile, err := os.OpenFile("/home/log.txt", os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		panic(err)
	}
	defer logFile.Close()
	mw := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(mw)
	masterIp = os.Getenv("MASTER_IP")
	log.Printf("masterIp: %v\n", masterIp)
	schedulerName = os.Getenv("SCHEDULER")
	log.Printf("cluster-scheduler: %v\n", schedulerName)
	clusterId = os.Getenv("CLUSTER_ID")
	log.Printf("clusterId: %v\n", clusterId)
	imagePullPolicy = os.Getenv("IMAGE_PULL_POLICY")
	log.Printf("scheduler imagePullPolicy: %v\n", imagePullPolicy)
	/*创建资源map，为每个节点创建一个key-value(ip:NodeResidualResource)*/
	nodeResidualResourceMap := make(k8sResource.ResidualResourceMap)
	nodeResidualMap = initNodeResidualResourceMap(nodeResidualResourceMap, masterIp)
	/*创建集群配置文件map(cluster+'i':"/config"+'i')*/
	//config := make(map[string]string,60)
	//i := 0
	//for key, value := range config {
	//	key = "cluster" + strconv.Itoa(i)
	//	value ="/config" + strconv.Itoa(i)
	//	config[key] = value
	//	i++
	//}
	//为informer创建chan
	stopper := make(chan struct{})
	defer close(stopper)

	//grpc Server
	waiter := sync.WaitGroup{} //创建sync.WaitGroup{}，保证所有go Routine结束，主线程再终止。
	waiter.Add(2)

	//gRPC远程连接调度器控制器
	go connectController(&waiter)
	//开启gRPC server
	go grpcServer(&waiter)

	/*创建每个集群的pod,node的Informer,返回PodLister，NodeLister，NsLister
	分别存放入各自的资源数组*/
	//i = 0
	//for key, _ := range config {
	//	clusterPodLister[i], clusterNodeLister[i], clusterNamespaceLister[i] = k8sResource.InitInformer(stopper, config[key])
	//	i++
	//}

	podLister, nodeLister, thisClusterNamespaceLister, thisClusterServiceLister, thisClusterPvcLister = k8sResource.InitInformer(stopper, "/kubelet.kubeconfig")
	//建立本集群的informer,本集群的scheduler namespace
	clientset = k8sResource.GetRemoteK8sClient()
	//err := createSchedulerNsInCurrentCluster(clientset)
	//if err != nil {
	//	panic(err.Error())
	//}
	defer runtime.HandleCrash()

	waiter.Wait()
}
