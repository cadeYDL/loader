当有多个请求过来，但是实际上只期望一个请求真实请求，其他请求正常结束的场景，就可以使用本组件
# 接口
```go
// Do
//  key:标记本次任务的唯一key
//  handler:真实执行的行为
//  options: 执行条件
func  Do(key string,handler func()error,options ...option)(bool,error)
// 执行时handler重新执行的重试次数
func WithRetry(retryCount int)option
// 在触发执行时xx时间后允许再次执行，可以选定为当前任务执行完之后开始计时还是执行前开始计时
func WithExpiration(expiration time.Du,handlerAfter bool)option
```


# 举个例子
```go

```