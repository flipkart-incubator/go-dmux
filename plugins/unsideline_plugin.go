package plugins

import (
	"fmt"
	gplugin "github.com/hashicorp/go-plugin"
	"log"
	"net/rpc"
)

type UnsidelineImpl interface {
	UnsidelineByKey(request UnsidelineByKeyRequest) (string, error)
}

type UnsidelineImplRPC struct {
	Client *rpc.Client
}

func (g *UnsidelineImplRPC) UnsidelineByKey(request UnsidelineByKeyRequest) (string, error) {
	var resp string
	fmt.Println("Calling UnsidelineByKey start : " + request.Key)
	err := g.Client.Call("Plugin.UnsidelineByKey", request, &resp)
	if err != nil {
		log.Printf(err.Error())
		return "", err
	}
	return resp, nil
}

type UnsidelineImplRPCServer struct {
	Impl UnsidelineImpl
}

func (s *UnsidelineImplRPCServer) UnsidelineByKey(request UnsidelineByKeyRequest, resp *string) error {
	var err error
	*resp, err = s.Impl.UnsidelineByKey(request)
	return err
}

type UnsidelineImplPlugin struct{}

func (UnsidelineImplPlugin) Server(*gplugin.MuxBroker) (interface{}, error) {
	return &UnsidelineImplRPC{}, nil
}

func (UnsidelineImplPlugin) Client(b *gplugin.MuxBroker, c *rpc.Client) (interface{}, error) {
	return &UnsidelineImplRPC{Client: c}, nil
}
