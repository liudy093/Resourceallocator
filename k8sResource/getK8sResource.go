package k8sResource

import (
	"fmt"

	_ "golang.org/x/net/context"
	_ "k8s.io/api/core/v1"
	corev1 "k8s.io/api/core/v1"

	//metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"log"
	"os"
	"path/filepath"
	"text/tabwriter"
	"time"

	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	v1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

// testing k8s cluster API by client-go packages
var clientset *kubernetes.Clientset

//	type resourceRequest struct {
//		MilliCpu int64
//		Memory int64
//		EphemeralStorage int64
//		Node1ResidualCpuPercentage int64
//		Node1ResidualMemPercentage int64
//		Node2ResidualCpuPercentage int64
//		Node2ResidualMemPercentage int64
//	}
var resourceRequestNum resourceRequest
var resourceAllocatableNum resourceRequest
var readK8sConfigNum uint64

type NodeResidualResource struct {
	MilliCpu uint64
	Memory   uint64
}
type ResidualResourceMap map[string]NodeResidualResource

// GetK8sApiResource
type resourceRequest struct {
	MilliCpu         uint64
	Memory           uint64
	EphemeralStorage uint64
}

func getK8sClient(configfile string) *kubernetes.Clientset {
	//k8sconfig= flag.String("k8sconfig","/opt/kubernetes/cfg/kubelet.kubeconfig","kubernetes config file path")
	//flag.Parse()
	//var k8sconfig string
	//if configfile == "/kubelet.kubeconfig" {
	//k8sconfig, err  := filepath.Abs(filepath.Dir("/opt/kubernetes/cfg/kubelet.kubeconfig"))
	if configfile == "/kubelet.kubeconfig" {
		k8sconfig, err := filepath.Abs(filepath.Dir("/etc/kubernetes/kubelet.kubeconfig"))
		if err != nil {
			panic(err.Error())
		}
		config, err := clientcmd.BuildConfigFromFlags("", k8sconfig+configfile)
		if err != nil {
			log.Println(err)
		}

		// 1.24.17版本的k8s集群，需要手动设置Host
		config.Host = "https://192.168.1.240:6443"

		clientset, err = kubernetes.NewForConfig(config)
		if err != nil {
			log.Fatalln(err)
		} else {
			log.Println(configfile)
			log.Println("Connect this cluster's k8s successfully for Informer.")
		}
		return clientset

	} else {
		k8sconfig, err := filepath.Abs(filepath.Dir("/root/.kube/config"))
		if err != nil {
			panic(err.Error())
		}
		config, err := clientcmd.BuildConfigFromFlags("", k8sconfig+configfile)
		if err != nil {
			log.Println(err)
		}
		clientset, err = kubernetes.NewForConfig(config)
		if err != nil {
			log.Fatalln(err)
		} else {
			log.Println(configfile)
			log.Println("Connect multi-cluster's k8s successfully.")
		}
		return clientset
	}
	//viper.AddConfigPath("/opt/kubernetes/cfg/")     //设置读取的文件路径
	//viper.SetConfigName("kubelet") //设置读取的文件名
	//viper.SetConfigType("yaml")   //设置文件的类型
	//k8sconfig := viper.ReadInConfig()
	//viper.WatchConfig()
}
func GetRemoteK8sClient() *kubernetes.Clientset {
	//k8sconfig= flag.String("k8sconfig","/opt/kubernetes/cfg/kubelet.kubeconfig","kubernetes config file path")
	//flag.Parse()
	//var k8sconfig string
	k8sconfig, err := filepath.Abs(filepath.Dir("/etc/kubernetes/kubelet.kubeconfig"))
	if err != nil {
		panic(err.Error())
	}
	config, err := clientcmd.BuildConfigFromFlags("", k8sconfig+"/kubelet.kubeconfig")
	if err != nil {
		log.Println(err)
	}

	// 1.24.17版本的k8s集群，需要手动设置Host
	config.Host = "https://192.168.1.240:6443"
	//viper.AddConfigPath("/opt/kubernetes/cfg/")     //设置读取的文件路径
	//viper.SetConfigName("kubelet") //设置读取的文件名
	//viper.SetConfigType("yaml")   //设置文件的类型
	//k8sconfig := viper.ReadInConfig()
	//viper.WatchConfig()
	clientset, err = kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalln(err)
	} else {
		log.Println("Connect k8s successfully for clientset.")
	}
	return clientset
}

/*遍历每个Node,通过podLister和nodeLister获取该Node的剩余资源，包括各集群的Master*/
func GetK8sEachNodeResource(podLister v1.PodLister, nodeLister v1.NodeLister, ResourceMap ResidualResourceMap) ResidualResourceMap {
	defer recoverPodListerFail()

	//从podlister,nodelister中获取所有items
	podList, err := podLister.List(labels.Everything())
	if err != nil {
		log.Println(err)
		panic(err.Error())
	}
	nodeList, err := nodeLister.List(labels.Everything())
	if err != nil {
		log.Println(err)
		panic(err.Error())
	}
	for key, val := range ResourceMap {
		currentNodePodsResourceSum := getEachNodePodsResourceRequest(podList, key)
		currentNodeAllocatableResource := getEachNodeAllocatableNum(nodeList, key)
		val.MilliCpu = currentNodeAllocatableResource.MilliCpu - currentNodePodsResourceSum.MilliCpu
		val.Memory = currentNodeAllocatableResource.Memory - currentNodePodsResourceSum.Memory
		ResourceMap[key] = NodeResidualResource{val.MilliCpu, val.Memory / 1024 / 1024}
		if val.MilliCpu != 0 || val.Memory != 0 {
			log.Println("Node:", key, "{", val.MilliCpu, val.Memory/1024/1024, "}")
		}
	}
	//log.Println(ResourceMap)
	return ResourceMap
}

// 遍历集群Node1节点所有pods，获取集群所有pods的资源request值
func getEachNodePodsResourceRequest(pods []*corev1.Pod, nodeName string) resourceRequest {
	defer recoverPodListerFail()
	resourceRequestNum := resourceRequest{0, 0, 0}
	for _, pod := range pods {
		if pod.Status.HostIP == nodeName {
			if (pod.Status.Phase == "Running") || (pod.Status.Phase == "Pending") {
				for _, container := range pod.Spec.Containers {
					resourceRequestNum.MilliCpu += uint64(container.Resources.Requests.Cpu().MilliValue())
					resourceRequestNum.Memory += uint64(container.Resources.Requests.Memory().Value())
					resourceRequestNum.EphemeralStorage += uint64(container.Resources.Requests.StorageEphemeral().Value())
				}
				for _, initContainer := range pod.Spec.InitContainers {
					resourceRequestNum.MilliCpu += uint64(initContainer.Resources.Requests.Cpu().MilliValue())
					resourceRequestNum.Memory += uint64(initContainer.Resources.Requests.Memory().Value())
				}
			}
		}
		//log.Printf("This %s's HostIP is %s .\n",pod.Name,pod.Status.HostIP)
	}
	return resourceRequestNum
}
func recoverPodListerFail() {
	if r := recover(); r != nil {
		log.Println("recovered from ", r)
	}
}

// 获取集群node的allocatable资源值
func getEachNodeAllocatableNum(nodes []*corev1.Node, nodeName string) resourceRequest {
	defer recoverPodListerFail()
	resourceAllocatableNum := resourceRequest{0, 0, 0}
	for _, nod := range nodes {
		//if nod.Name[0:9] != "admiralty" {
		//if nod.Name[0:12] == "k8s-2-node-1" {
		if nod.Name == nodeName {
			resourceAllocatableNum.MilliCpu = uint64(nod.Status.Allocatable.Cpu().MilliValue())
			resourceAllocatableNum.Memory = uint64(nod.Status.Allocatable.Memory().Value())
			resourceAllocatableNum.EphemeralStorage = uint64(nod.Status.Allocatable.StorageEphemeral().Value())
		}
		//log.Printf("This is %s.\n", nod.Name)
		//log.Printf("Current node1: %s,Allocatable CpuNum = %dm,Allocatable MemNum = %dMi\n",
		//	nod.Name, nod.Status.Allocatable.Cpu().MilliValue(),nod.Status.Allocatable.Memory().Value()/1024/1024)
	}
	return resourceAllocatableNum
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

func recoverGetK8sApiResourceFail() {
	if r := recover(); r != nil {
		log.Println("recovered from GetK8sApiResourceFail", r)
	}
}
func GetK8sApiResource(podLister v1.PodLister, nodeLister v1.NodeLister, masterIp string) resourceRequest {
	defer recoverGetK8sApiResourceFail()
	//get k8s cluster config  client
	//clientset := getK8sClient()
	//get pods info in whole master cluster
	//pods, err := clientset.CoreV1().Pods("").List(metav1.ListOptions{})
	//if err != nil {
	//	log.Println(err.Error())
	//}
	//print all pods info in whole master cluster
	//printK8SPodInfo(*pods)
	//从podlister,nodelister中获取所有items
	podList, err := podLister.List(labels.Everything())
	if err != nil {
		log.Println(err)
	}
	nodeList, err := nodeLister.List(labels.Everything())
	if err != nil {
		log.Println(err)
	}
	//get nodes info in whole master cluster
	//nodes, err := clientset.CoreV1().Nodes().List(metav1.ListOptions{})
	//if err != nil {
	//	log.Println(err.Error())
	//}
	// print nodes info in whole master cluster
	//printK8SNodeInfo(*nodes)
	//fmt.Println(pods.Items[0].Name)
	//fmt.Println(pods.Items[0].CreationTimestamp)
	//fmt.Println(pods.Items[0].Labels)
	//fmt.Println(pods.Items[0].Namespace)
	//fmt.Println(pods.Items[0].Status.PodIP)
	//fmt.Println(pods.Items[0].Status.StartTime)
	//fmt.Println(pods.Items[0].Status.Phase)
	//fmt.Println(pods.Items[0].Status.ContainerStatuses[0].RestartCount)   //重启次数
	//fmt.Println(pods.Items[0].Status.ContainerStatuses[0].Image) //获取重启时间
	//fmt.Println(pods.Items[0].Status.ContainerStatuses[0].Size())
	//get nodes info
	//for _, node := range nodes.Items {
	//	//fmt.Printf("Name: %s, Status: %s, CreateTime: %s\n", node.ObjectMeta.Name, node.Status.Phase, node.ObjectMeta.CreationTimestamp)
	//
	//}
	//fmt.Println(nodes.Items[1].Name)
	//fmt.Println(nodes.Items[1].CreationTimestamp)    //加入集群时间
	//fmt.Println(nodes.Items[1].Status.NodeInfo)
	//fmt.Println(nodes.Items[1].Status.Conditions[len(nodes.Items[0].Status.Conditions)-1].Type)
	//fmt.Println("*********************************")
	//fmt.Println(nodes.Items[1].Status.Allocatable.Cpu().String())
	//fmt.Println(nodes.Items[1].Status.Allocatable.Memory().String())
	//fmt.Println(nodes.Items[1].Status.Capacity.Cpu().String())
	//fmt.Println(nodes.Items[1].Status.Capacity.Memory().String())
	//fmt.Println("------------------------------------------------------------------")
	allPodsResourceRequest := getPodsResourceRequest(podList, masterIp)
	log.Printf("allPodsResourceRequest cpuNum =%dm,allPodsResourceRequest memNum =%dMi\n",
		allPodsResourceRequest.MilliCpu, allPodsResourceRequest.Memory/1024/1024)
	//
	//Node1PodsResourceRequest := getNode1PodsResourceRequest(podList)
	//log.Printf("Node1PodsResourceRequest cpuNum =%dm,Node1PodsResourceRequest memNum =%dMi\n",
	//	Node1PodsResourceRequest.MilliCpu,Node1PodsResourceRequest.Memory/1024/1024)
	//Node2PodsResourceRequest := getNode2PodsResourceRequest(podList)
	//log.Printf("Node2PodsResourceRequest cpuNum =%dm,Node2PodsResourceRequest memNum =%dMi\n",
	//	Node2PodsResourceRequest.MilliCpu,Node2PodsResourceRequest.Memory/1024/1024)
	allNodesResourceAllocatable := getNodesAllocatableNum(nodeList, masterIp)
	log.Printf("allNodesResourceAllocatable cpuNum =%dm,allNodesResourceAllocatable memNum =%dMi\n",
		allNodesResourceAllocatable.MilliCpu, allNodesResourceAllocatable.Memory/1024/1024)
	//Node1ResourceAllocatable := getNode1AllocatableNum(nodeList)
	//log.Printf("Node1ResourceAllocatable cpuNum =%dm,Node1ResourceAllocatable memNum =%dMi\n",
	//	Node1ResourceAllocatable.MilliCpu,Node1ResourceAllocatable.Memory/1024/1024)
	//Node2ResourceAllocatable := getNode2AllocatableNum(nodeList)
	//log.Printf("Node2ResourceAllocatable cpuNum =%dm,Node2ResourceAllocatable memNum =%dMi\n",
	//	Node2ResourceAllocatable.MilliCpu,Node2ResourceAllocatable.Memory/1024/1024)

	//剩余资源
	//fmt.Printf("Residual Cpu = %dm\n", allNodesResourceAllocatable.MilliCpu-allPodsResourceRequest.MilliCpu)
	//fmt.Printf("Residual Memory = %dMi\n", (allNodesResourceAllocatable.Memory-allPodsResourceRequest.Memory)/1024/1024)
	readK8sConfigNum++
	log.Println("--------------------------------------------------------")
	log.Printf("get k8s resource in %dth time through informer.\n", readK8sConfigNum)
	if (allPodsResourceRequest.MilliCpu >= allNodesResourceAllocatable.MilliCpu) ||
		(allPodsResourceRequest.Memory >= allNodesResourceAllocatable.Memory) {
		return resourceRequest{MilliCpu: 0, Memory: 0, EphemeralStorage: 0}
	} else {
		return resourceRequest{MilliCpu: allNodesResourceAllocatable.MilliCpu - allPodsResourceRequest.MilliCpu,
			Memory:           (allNodesResourceAllocatable.Memory - allPodsResourceRequest.Memory) / 1024 / 1024,
			EphemeralStorage: allNodesResourceAllocatable.EphemeralStorage - allPodsResourceRequest.EphemeralStorage,
		}
	}
	//Node1ResidualCpuPercentage:(Node1ResourceAllocatable.MilliCpu-Node1PodsResourceRequest.MilliCpu)*100/Node1ResourceAllocatable.MilliCpu,
	//Node1ResidualMemPercentage:(Node1ResourceAllocatable.Memory-Node1PodsResourceRequest.Memory)*100/Node1ResourceAllocatable.Memory,
	//Node2ResidualCpuPercentage:(Node2ResourceAllocatable.MilliCpu-Node2PodsResourceRequest.MilliCpu)*100/Node2ResourceAllocatable.MilliCpu,
	//Node2ResidualMemPercentage:(Node2ResourceAllocatable.Memory-Node2PodsResourceRequest.Memory)*100/Node2ResourceAllocatable.Memory}
}
func printK8SPodInfo(pods corev1.PodList) {
	fmt.Println("--------------------------------------------------------------------------------")

	const format = "%v\t%v\t%v\t%v\t%v\t%v\t\n"
	tw := new(tabwriter.Writer).Init(os.Stdout, 0, 8, 2, ' ', 0)
	fmt.Fprintf(tw, format, "Namespace", "Name", "Status", "Age", "PodIP", "Node")
	fmt.Fprintf(tw, format, "------", "------", "-----", "----", "-------", "-----")
	for _, r := range pods.Items {
		fmt.Fprintf(tw, format, r.Namespace, r.Name, r.Status.Phase, r.Status.StartTime, r.Status.PodIP, r.Spec.NodeName)
	}
	tw.Flush()

}
func printK8SNodeInfo(nodes corev1.NodeList) {
	fmt.Println("--------------------------------------------------------------------------------")
	const format1 = "%v\t%v\t%v\t%v\t%v\t%v\t%v\t\n"
	tw1 := new(tabwriter.Writer).Init(os.Stdout, 0, 8, 2, ' ', 0)
	fmt.Fprintf(tw1, format1, "Name", "Status", "CreateTime",
		"Allocatable.Cpu", "Allocatable.Memory", "Capacity.Cpu", "Capacity.Memory")
	fmt.Fprintf(tw1, format1, "------", "------", "---------", "----", "----", "----", "----")
	for _, r := range nodes.Items {
		fmt.Fprintf(tw1, format1, r.Name, r.Status.Conditions[len(r.Status.Conditions)-1].Type,
			r.CreationTimestamp, r.Status.Allocatable.Cpu(),
			r.Status.Allocatable.Memory(), r.Status.Capacity.Cpu(),
			r.Status.Capacity.Memory())
	}
	tw1.Flush()
}

// 遍历集群所有pods，获取集群所有pods的资源request值
func getPodsResourceRequest(pods []*corev1.Pod, clusterMasterIp string) resourceRequest {
	defer recoverPodListerFail()
	resourceRequestNum := resourceRequest{0, 0, 0}
	for _, pod := range pods {
		if pod.Status.HostIP != clusterMasterIp {
			if (pod.Status.Phase == "Running") || (pod.Status.Phase == "Pending") {
				for _, container := range pod.Spec.Containers {
					resourceRequestNum.MilliCpu += uint64(container.Resources.Requests.Cpu().MilliValue())
					resourceRequestNum.Memory += uint64(container.Resources.Requests.Memory().Value())
					resourceRequestNum.EphemeralStorage += uint64(container.Resources.Requests.StorageEphemeral().Value())
				}
				for _, initContainer := range pod.Spec.InitContainers {
					resourceRequestNum.MilliCpu += uint64(initContainer.Resources.Requests.Cpu().MilliValue())
					resourceRequestNum.Memory += uint64(initContainer.Resources.Requests.Memory().Value())
				}
				//fmt.Printf("cpu = %d\n",resourceRequestNum.MilliCpu)
				//fmt.Printf("mem = %d\n",resourceRequestNum.Memory/1024/1024)
			}
		}
	}
	return resourceRequestNum
}

// 遍历集群Node1节点所有pods，获取集群所有pods的资源request值
func getNode1PodsResourceRequest(pods []*corev1.Pod) resourceRequest {
	defer recoverPodListerFail()
	resourceRequestNum := resourceRequest{0, 0, 0}
	for _, pod := range pods {
		if pod.Status.HostIP == "121.250.173.194" {
			if pod.Status.Phase == "Running" {
				for _, container := range pod.Spec.Containers {
					resourceRequestNum.MilliCpu += uint64(container.Resources.Requests.Cpu().MilliValue())
					resourceRequestNum.Memory += uint64(container.Resources.Requests.Memory().Value())
					resourceRequestNum.EphemeralStorage += uint64(container.Resources.Requests.StorageEphemeral().Value())
				}
				for _, initContainer := range pod.Spec.InitContainers {
					resourceRequestNum.MilliCpu += uint64(initContainer.Resources.Requests.Cpu().MilliValue())
					resourceRequestNum.Memory += uint64(initContainer.Resources.Requests.Memory().Value())
				}
				//fmt.Printf("cpu = %d\n",resourceRequestNum.MilliCpu)
				//fmt.Printf("mem = %d\n",resourceRequestNum.Memory/1024/1024)
			}
		}
		//log.Printf("This %s's HostIP is %s .\n",pod.Name,pod.Status.HostIP)
	}
	return resourceRequestNum
}

// 遍历集群Node2节点所有pods，获取集群所有pods的资源request值
func getNode2PodsResourceRequest(pods []*corev1.Pod) resourceRequest {
	defer recoverPodListerFail()
	resourceRequestNum := resourceRequest{0, 0, 0}
	for _, pod := range pods {
		if pod.Status.HostIP == "121.250.173.195" {
			if pod.Status.Phase == "Running" {
				for _, container := range pod.Spec.Containers {
					resourceRequestNum.MilliCpu += uint64(container.Resources.Requests.Cpu().MilliValue())
					resourceRequestNum.Memory += uint64(container.Resources.Requests.Memory().Value())
					resourceRequestNum.EphemeralStorage += uint64(container.Resources.Requests.StorageEphemeral().Value())
				}
				for _, initContainer := range pod.Spec.InitContainers {
					resourceRequestNum.MilliCpu += uint64(initContainer.Resources.Requests.Cpu().MilliValue())
					resourceRequestNum.Memory += uint64(initContainer.Resources.Requests.Memory().Value())
				}
				//fmt.Printf("cpu = %d\n",resourceRequestNum.MilliCpu)
				//fmt.Printf("mem = %d\n",resourceRequestNum.Memory/1024/1024)
			}
		}
		//log.Printf("This %s's HostIP is %s .\n",pod.Name,pod.Status.HostIP)
	}
	return resourceRequestNum
}

// 获取集群所有nodes的allocatable资源值
func getNodesAllocatableNum(nodes []*corev1.Node, clusterMasterIp string) resourceRequest {
	resourceAllocatableNum := resourceRequest{0, 0, 0}
	for _, nod := range nodes {
		//if nod.Name[0:9] != "admiralty" {
		//if nod.Name[0:10] == "k8s-2-node" {
		//if (nod.Name != "121.250.173.190")&&(nod.Name != "121.250.173.191")&&(nod.Name != "121.250.173.192"){
		if nod.Name != clusterMasterIp {
			resourceAllocatableNum.MilliCpu += uint64(nod.Status.Allocatable.Cpu().MilliValue())
			resourceAllocatableNum.Memory += uint64(nod.Status.Allocatable.Memory().Value())
			resourceAllocatableNum.EphemeralStorage += uint64(nod.Status.Allocatable.StorageEphemeral().Value())
			//log.Printf("Node-%s: Allocatable CpuNum = %dm,Allocatable MemNum = %dMi\n",
			//nod.Name, nod.Status.Allocatable.Cpu().MilliValue(),nod.Status.Allocatable.Memory().Value()/1024/1024)
			//}
		}
	}
	//log.Printf("All nodes: Allocatable CpuNum = %dm,Allocatable MemNum = %dMi\n",
	//	resourceAllocatableNum.MilliCpu,resourceAllocatableNum.Memory/1024/1024)
	return resourceAllocatableNum
}

// 获取集群node1的allocatable资源值
func getNode1AllocatableNum(nodes []*corev1.Node) resourceRequest {

	defer recoverPodListerFail()
	resourceAllocatableNum := resourceRequest{0, 0, 0}
	for _, nod := range nodes {
		//if nod.Name[0:9] != "admiralty" {
		//if nod.Name[0:12] == "k8s-2-node-1" {
		if nod.Name == "121.250.173.194" {
			resourceAllocatableNum.MilliCpu = uint64(nod.Status.Allocatable.Cpu().MilliValue())
			resourceAllocatableNum.Memory = uint64(nod.Status.Allocatable.Memory().Value())
			resourceAllocatableNum.EphemeralStorage = uint64(nod.Status.Allocatable.StorageEphemeral().Value())
		}
		//log.Printf("This is %s.\n", nod.Name)
		//log.Printf("Current node1: %s,Allocatable CpuNum = %dm,Allocatable MemNum = %dMi\n",
		//	nod.Name, nod.Status.Allocatable.Cpu().MilliValue(),nod.Status.Allocatable.Memory().Value()/1024/1024)
	}
	return resourceAllocatableNum
}

// 获取集群所有node2的allocatable资源值
func getNode2AllocatableNum(nodes []*corev1.Node) resourceRequest {

	resourceAllocatableNum := resourceRequest{0, 0, 0}
	for _, nod := range nodes {
		//if nod.Name[0:9] != "admiralty" {
		//if nod.Name[0:12] == "k8s-2-node-2" {
		if nod.Name == "121.250.173.195" {
			resourceAllocatableNum.MilliCpu = uint64(nod.Status.Allocatable.Cpu().MilliValue())
			resourceAllocatableNum.Memory = uint64(nod.Status.Allocatable.Memory().Value())
			resourceAllocatableNum.EphemeralStorage = uint64(nod.Status.Allocatable.StorageEphemeral().Value())
		}
		//log.Printf("This is %s.\n", nod.Name)
		//log.Printf("Current node2: %s,Allocatable CpuNum = %dm,Allocatable MemNum = %dMi\n",
		//	nod.Name, nod.Status.Allocatable.Cpu().MilliValue(),nod.Status.Allocatable.Memory().Value()/1024/1024)
	}
	return resourceAllocatableNum
}

// 创建pod的Informer,node的Informer,namespace的Informer,service的Informer,pvc的Informer
func InitInformer(stop chan struct{}, configfile string) (v1.PodLister,
	v1.NodeLister, v1.NamespaceLister, v1.ServiceLister, v1.PersistentVolumeClaimLister) {
	//连接k8s集群apiserver，创建clientset
	informerClientset := getK8sClient(configfile)
	//初始化informer
	factory := informers.NewSharedInformerFactory(informerClientset, time.Second*3)

	//测试，打印config.host
	log.Println(informerClientset.RESTClient().Get().URL().Host)

	//创建pod的Informer
	podInformer := factory.Core().V1().Pods()
	informerPod := podInformer.Informer()

	//创建node的Informer
	nodeInformer := factory.Core().V1().Nodes()
	informerNode := nodeInformer.Informer()

	//创建namespace的Informer
	namespaceInformer := factory.Core().V1().Namespaces()
	informerNamespace := namespaceInformer.Informer()

	//创建service的Informer
	serviceInformer := factory.Core().V1().Services()
	informerService := serviceInformer.Informer()
	//创建PVC的Informer
	pvcInformer := factory.Core().V1().PersistentVolumeClaims()
	informerPvc := pvcInformer.Informer()
	//创建pod，node,namespace,service的Lister
	podInformerLister := podInformer.Lister()
	nodeInformerLister := nodeInformer.Lister()
	namespaceInformerLister := namespaceInformer.Lister()
	serviceInformerLister := serviceInformer.Lister()
	pvcInformerLister := pvcInformer.Lister()

	go factory.Start(stop)
	//从apiserver同步资源pods，即list
	if !cache.WaitForCacheSync(stop, informerPod.HasSynced) {
		runtime.HandleError(fmt.Errorf("Timed out waiting for caches to sync"))
		return nil, nil, nil, nil, nil
	}
	//从apiserver同步资源nodes，即list
	if !cache.WaitForCacheSync(stop, informerNode.HasSynced) {
		runtime.HandleError(fmt.Errorf("Timed out waiting for caches to sync"))
		return nil, nil, nil, nil, nil
	}
	//从apiserver同步资源namespaces，即list
	if !cache.WaitForCacheSync(stop, informerNamespace.HasSynced) {
		runtime.HandleError(fmt.Errorf("Timed out waiting for caches to sync"))
		return nil, nil, nil, nil, nil
	}
	//从apiserver同步资源service，即list
	if !cache.WaitForCacheSync(stop, informerService.HasSynced) {
		runtime.HandleError(fmt.Errorf("Timed out waiting for caches to sync"))
		return nil, nil, nil, nil, nil
	}
	//从apiserver同步资源pvc，即list
	if !cache.WaitForCacheSync(stop, informerPvc.HasSynced) {
		runtime.HandleError(fmt.Errorf("Timed out waiting for caches to sync"))
		return nil, nil, nil, nil, nil
	}

	// 使用自定义pod events handler
	informerPod.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    onPodAdd,
		UpdateFunc: onPodUpdate,
		DeleteFunc: onPodDelete,
	})
	// 使用自定义node events handler
	informerNode.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    onNodeAdd,
		UpdateFunc: onNodeUpdate,
		DeleteFunc: onNodeDelete,
	})
	// 使用自定义namespace events handler
	informerNamespace.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    onNamespaceAdd,
		UpdateFunc: onNamespaceUpdate,
		DeleteFunc: onNamespaceDelete,
	})
	// 使用自定义service events handler
	informerService.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    onServiceAdd,
		UpdateFunc: onServiceUpdate,
		DeleteFunc: onServiceDelete,
	})
	// 使用自定义pvc events handler
	informerService.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    onPvcAdd,
		UpdateFunc: onPvcUpdate,
		DeleteFunc: onPvcDelete,
	})
	return podInformerLister, nodeInformerLister, namespaceInformerLister, serviceInformerLister, pvcInformerLister
}
func onPodAdd(obj interface{}) {
	//pod := obj.(*corev1.Pod)
	//log.Println("add a pod:", pod.Name)
}
func onPodUpdate(old interface{}, current interface{}) {
	//log.Println("updating..............")
}
func onPodDelete(obj interface{}) {
	//pod := obj.(*corev1.Pod)
	//log.Println("delete a pod:", pod.Name)
}
func onNodeAdd(obj interface{}) {
	//node := obj.(*corev1.Node)
	//log.Println("add a node:", node.Name)
}
func onNodeUpdate(old interface{}, current interface{}) {
	//log.Println("updating..............")
}
func onNodeDelete(obj interface{}) {
	//node := obj.(*corev1.Node)
	//log.Println("delete a Node:", node.Name)
}
func onNamespaceAdd(obj interface{}) {
	//namespace := obj.(*corev1.Namespace)
	//log.Println("add a namespace:", namespace.Name)
}
func onNamespaceUpdate(old interface{}, current interface{}) {
	//log.Println("updating..............")
}
func onNamespaceDelete(obj interface{}) {
	//namespace := obj.(*corev1.Namespace)
	//log.Println("delete a namespace:", namespace.Name)
}
func onServiceAdd(obj interface{}) {
	//service := obj.(*corev1.Service)
	//log.Println("add a service:", service.Name)
}
func onServiceUpdate(old interface{}, current interface{}) {
	//log.Println("updating..............")
}
func onServiceDelete(obj interface{}) {
	//service := obj.(*corev1.Service)
	//log.Println("delete a service:", service.Name)
}
func onPvcAdd(obj interface{}) {
	//service := obj.(*corev1.Service)
	//log.Println("add a service:", service.Name)
}
func onPvcUpdate(old interface{}, current interface{}) {
	//log.Println("updating..............")
}
func onPvcDelete(obj interface{}) {
	//service := obj.(*corev1.Service)
	//log.Println("delete a service:", service.Name)
}
