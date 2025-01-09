package mqrpc

type functionCall struct {
	Name string `json:"name"`
	Data any    `json:"data"`
}
