package main

import (
	"fmt"
	"regexp"
)

var inputVector = []string{
	"\n\x13TESTSET_PART_NUMBER\x12\x013",
	"\n\x0eMEMORY_CONSUME\x12\x0264",
	"\n\vCPU_CONSUME\x12\x03100",
}

func main() {
	envVars, err := ParseEnvVarsFromInputVector(inputVector)
	if err != nil {
		fmt.Println("解析环境变量失败:", err)
		return
	}

	fmt.Println("解析环境变量成功: %#v\n", envVars)
}

func ParseEnvVarsFromInputVector(inputVector []string) (map[string]string, error) {
	envVars := make(map[string]string)

	// 正则表达式用于匹配键和值
	re := regexp.MustCompile(`\n.*?(\w+).*?\x12.*?(\d+)`)

	for _, item := range inputVector {
		matches := re.FindStringSubmatch(item)
		if len(matches) == 3 {
			// 第一个捕获组是键，第二个是值
			key := matches[1]
			value := matches[2]
			envVars[key] = value
		}
	}

	return envVars, nil
}
