package structs

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const (
	/**
	 * Code定义
	 */
	ERROR_400 = "参数有误"
	ERROR_401 = "无权操作"
	ERROR_500 = "系统异常"

	// 参数校验Code定义：1000 ~ 1099
	ERROR_1001 = "%s不能为空"
	ERROR_1002 = "%s的值只能为[%s]"
	ERROR_1003 = "%s的长度不能超过[%s]"
	ERROR_1004 = "%s[%s]无效"
	ERROR_1005 = "%s不能大于%s[%s，%s]"
	ERROR_1006 = "%s不能重复"
	ERROR_1007 = "%s必须是正整数"
	ERROR_1008 = "%s必须大于%s"
	ERROR_1009 = "%s必须大于等于%s"
	ERROR_1010 = "%s格式不正确[%s]"
	ERROR_1011 = "存在循环依赖，创建失败"

	// 共通业务Code定义：1100 ~ 1199
	ERROR_1100 = "%s[%s]已经存在"
	ERROR_1101 = "%s[%s]不存在"
	ERROR_1102 = "%s[%s]不能重复"
	ERROR_1103 = "结束时间不能小于开始时间"
	ERROR_1104 = "%s[%s]不支持"
	ERROR_1105 = "%s[%s]已被使用,无法删除"
	ERROR_1106 = "%s[%s]是%s,无法%s"
	ERROR_1107 = "执行%s[%s]失败：%s"
	ERROR_1108 = "%s[%s]存在%s,无法删除"
	ERROR_1109 = "%s不存在"

	errNoLeader                   = "No cluster leader"
	errNotReadyForConsistentReads = "Not ready to serve consistent reads"
	errNoRegionPath               = "No path to region"
	errTokenNotFound              = "ACL token not found"
	errPermissionDenied           = "Permission denied"
	errJobRegistrationDisabled    = "Plan registration, dispatch, and scale are disabled by the scheduler configuration"
	errNoNodeConn                 = "No path to node"
	errUnknownMethod              = "Unknown rpc method"
	errNodeLacksRpc               = "Node does not support RPC; requires 0.8 or later"
	errIncompatibleFiltering      = "Filter expression cannot be used with other filter parameters"

	// Prefix based errors that are used to check if the error is of a given
	// type. These errors should be created with the associated constructor.
	ErrUnknownAllocationPrefix   = "Unknown allocation"
	ErrUnknownNodePrefix         = "Unknown node"
	ErrUnknownPlanPrefix         = "Unknown plan"
	ErrUnknownEvaluationPrefix   = "Unknown evaluation"
	ErrUnknownDeploymentPrefix   = "Unknown deployment"
	ErrorNotOutStandingContent   = "evaluation is not outstanding"
	ErrTokenMismatchContent      = "evaluation token does not match"
	ErrNackTimeoutReachedContent = "evaluation nack timeout reached"

	errRPCCodedErrorPrefix = "RPC Error:: "
)

var (
	Err400WrongParam = errors.New(ERROR_400)
	Err401           = errors.New(ERROR_401)
	Err500           = errors.New(ERROR_500)

	ErrNoLeader                   = errors.New(errNoLeader)
	ErrNotReadyForConsistentReads = errors.New(errNotReadyForConsistentReads)
	ErrNoRegionPath               = errors.New(errNoRegionPath)
	ErrJobRegistrationDisabled    = errors.New(errJobRegistrationDisabled)
	ErrNoNodeConn                 = errors.New(errNoNodeConn)
	ErrUnknownMethod              = errors.New(errUnknownMethod)
	ErrNodeLacksRpc               = errors.New(errNodeLacksRpc)
	ErrNotOutstanding             = errors.New(ErrorNotOutStandingContent)
	ErrTokenMismatch              = errors.New(ErrTokenMismatchContent)
	ErrNackTimeoutReached         = errors.New(ErrNackTimeoutReachedContent)

	ErrUnknownNode = errors.New(ErrUnknownNodePrefix)
)

// IsErrNoLeader returns whether the error is due to there being no leader.
func IsErrNoLeader(err error) bool {
	return err != nil && strings.Contains(err.Error(), errNoLeader)
}

// IsErrNoRegionPath returns whether the error is due to there being no path to
// the given region.
func IsErrNoRegionPath(err error) bool {
	return err != nil && strings.Contains(err.Error(), errNoRegionPath)
}

// IsErrTokenNotFound returns whether the error is due to the passed token not
// being resolvable.
func IsErrTokenNotFound(err error) bool {
	return err != nil && strings.Contains(err.Error(), errTokenNotFound)
}

// IsErrPermissionDenied returns whether the error is due to the operation not
// being allowed due to lack of permissions.
func IsErrPermissionDenied(err error) bool {
	return err != nil && strings.Contains(err.Error(), errPermissionDenied)
}

// IsErrNoNodeConn returns whether the error is due to there being no path to
// the given node.
func IsErrNoNodeConn(err error) bool {
	return err != nil && strings.Contains(err.Error(), errNoNodeConn)
}

// IsErrUnknownMethod returns whether the error is due to the operation not
// being allowed due to lack of permissions.
func IsErrUnknownMethod(err error) bool {
	return err != nil && strings.Contains(err.Error(), errUnknownMethod)
}

func IsErrRPCCoded(err error) bool {
	return err != nil && strings.HasPrefix(err.Error(), errRPCCodedErrorPrefix)
}

// NewErrUnknownAllocation returns a new error caused by the allocation being
// unknown.
func NewErrUnknownAllocation(allocID string) error {
	return fmt.Errorf("%s %q", ErrUnknownAllocationPrefix, allocID)
}

// NewErrUnknownNode returns a new error caused by the node being unknown.
func NewErrUnknownNode(nodeID string) error {
	return fmt.Errorf("%s %q", ErrUnknownNodePrefix, nodeID)
}

// NewErrUnknownJob returns a new error caused by the plan being unknown.
func NewErrUnknownJob(jobID string) error {
	return fmt.Errorf("%s %q", ErrUnknownPlanPrefix, jobID)
}

// NewErrUnknownEvaluation returns a new error caused by the evaluation being
// unknown.
func NewErrUnknownEvaluation(evaluationID string) error {
	return fmt.Errorf("%s %q", ErrUnknownEvaluationPrefix, evaluationID)
}

// NewErrUnknownDeployment returns a new error caused by the deployment being
// unknown.
func NewErrUnknownDeployment(deploymentID string) error {
	return fmt.Errorf("%s %q", ErrUnknownDeploymentPrefix, deploymentID)
}

// IsErrUnknownAllocation returns whether the error is due to an unknown
// allocation.
func IsErrUnknownAllocation(err error) bool {
	return err != nil && strings.Contains(err.Error(), ErrUnknownAllocationPrefix)
}

// IsErrUnknownNode returns whether the error is due to an unknown
// node.
func IsErrUnknownNode(err error) bool {
	return err != nil && strings.Contains(err.Error(), ErrUnknownNodePrefix)
}

// IsErrUnknownJob returns whether the error is due to an unknown
// plan.
func IsErrUnknownJob(err error) bool {
	return err != nil && strings.Contains(err.Error(), ErrUnknownPlanPrefix)
}

// IsErrUnknownEvaluation returns whether the error is due to an unknown
// evaluation.
func IsErrUnknownEvaluation(err error) bool {
	return err != nil && strings.Contains(err.Error(), ErrUnknownEvaluationPrefix)
}

// IsErrNodeLacksRpc returns whether error is due to a Jobpool server being
// unable to connect to a client node because the client is too old (pre-v0.8).
func IsErrNodeLacksRpc(err error) bool {
	return err != nil && strings.Contains(err.Error(), errNodeLacksRpc)
}

func IsErrNoSuchFileOrDirectory(err error) bool {
	return err != nil && strings.Contains(err.Error(), "no such file or directory")
}

// NewErrRPCCoded wraps an RPC error with a code to be converted to HTTP status
// code
func NewErrRPCCoded(code int, msg string) error {
	return fmt.Errorf("%s%d,%s", errRPCCodedErrorPrefix, code, msg)
}

// NewErrRPCCodedf wraps an RPC error with a code to be converted to HTTP
// status code.
func NewErrRPCCodedf(code int, format string, args ...interface{}) error {
	msg := fmt.Sprintf(format, args...)
	return fmt.Errorf("%s%d,%s", errRPCCodedErrorPrefix, code, msg)
}

// CodeFromRPCCodedErr returns the code and message of error if it's an RPC error
// created through NewErrRPCCoded function.  Returns `ok` false if error is not
// an rpc error
func CodeFromRPCCodedErr(err error) (code int, msg string, ok bool) {
	if err == nil || !strings.HasPrefix(err.Error(), errRPCCodedErrorPrefix) {
		return 0, "", false
	}

	headerLen := len(errRPCCodedErrorPrefix)
	parts := strings.SplitN(err.Error()[headerLen:], ",", 2)
	if len(parts) != 2 {
		return 0, "", false
	}

	code, err = strconv.Atoi(parts[0])
	if err != nil {
		return 0, "", false
	}

	return code, parts[1], true
}

func NewErr1001Blank(name string) error {
	return fmt.Errorf(ERROR_1001, name)
}

func NewErr1002LimitValue(name string, value string) error {
	return fmt.Errorf(ERROR_1002, name, value)
}

func NewErr1003Size(name string, size string) error {
	return fmt.Errorf(ERROR_1003, name, size)
}

func NewErr1004Invalid(name string, value string) error {
	return fmt.Errorf(ERROR_1004, name, value)
}

func NewErr1005Range(name string, value string, start string, end string) error {
	return fmt.Errorf(ERROR_1005, name, value, start, end)
}
func NewErr1006Repeat(name string) error {
	return fmt.Errorf(ERROR_1006, name)
}
func NewErr1007Int(name string) error {
	return fmt.Errorf(ERROR_1007, name)
}
func NewErr1008Above(name string, value string) error {
	return fmt.Errorf(ERROR_1008, name, value)
}

func NewErr1009AboveOn(name string, value string) error {
	return fmt.Errorf(ERROR_1009, name, value)
}

func NewErr1010Format(name string, value string) error {
	return fmt.Errorf(ERROR_1010, name, value)
}

func NewErr1011Circle() error {
	return errors.New(ERROR_1011)
}

func NewErr1100Exist(name string, value string) error {
	return fmt.Errorf(ERROR_1100, name, value)
}

func NewErr1101None(name string, value string) error {
	return fmt.Errorf(ERROR_1101, name, value)
}

func NewErr1102Repeat(name string, value string) error {
	return fmt.Errorf(ERROR_1102, name, value)
}

func NewErr1103StartEnd() error {
	return errors.New(ERROR_1103)
}

func NewErr1104NotSupport(name string, value string) error {
	return fmt.Errorf(ERROR_1104, name, value)
}

func NewErr1105Used(name string, value string) error {
	return fmt.Errorf(ERROR_1105, name, value)
}

func NewErr1106Cannot(name string, value string, real string, espect string) error {
	return fmt.Errorf(ERROR_1106, name, value, real, espect)
}

func NewErr1107FailExecute(name string, value string, msg string) error {
	return fmt.Errorf(ERROR_1107, name, value, msg)
}

func NewErr1108DeleteAble(name string, value string, msg string) error {
	return fmt.Errorf(ERROR_1108, name, value, msg)
}

func NewErr1109NotExist(name string) error {
	return fmt.Errorf(ERROR_1109, name)
}
