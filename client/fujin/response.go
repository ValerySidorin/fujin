package fujin

type AckResponse struct {
	Err             error
	AckMsgResponses []AckMsgResponse
}

type AckMsgResponse struct {
	MsgID []byte
	Err   error
}

type SubscribeResponse struct {
	SubID byte
	Err   error
}
