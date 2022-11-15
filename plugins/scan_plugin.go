package plugins

import (
	gplugin "github.com/hashicorp/go-plugin"
	"log"
	"net/rpc"
	"strconv"
)

type ScanImpl interface {
	ScanWithStartRowEndRow(request ScanWithStartRowEndRowRequest) ([]string, error)
	ScanWithStartTimeEndTime(request ScanWithStartTimeEndTimeRequest) ([]string, error)
}

type ScanImplRPC struct {
	Client *rpc.Client
}

func (g *ScanImplRPC) ScanWithStartRowEndRow(request ScanWithStartRowEndRowRequest) ([]string, error) {
	var resp []string
	log.Printf("Calling ScanWithStartRowEndRow start : " + request.StartKey + " end: " + request.EndKey)
	err := g.Client.Call("Plugin.ScanWithStartRowEndRow", request, &resp)
	if err != nil {
		log.Fatal(err.Error())
		return nil, err
	}
	return resp, nil
}

func (g *ScanImplRPC) ScanWithStartTimeEndTime(request ScanWithStartTimeEndTimeRequest) ([]string, error) {
	var resp []string
	log.Printf("Calling ScanWithStartTimeEndTime start : " + strconv.FormatInt(request.StartTime, 10) +
		" end: " + strconv.FormatInt(request.EndTime, 10))
	err := g.Client.Call("Plugin.ScanWithStartTimeEndTime", request, &resp)
	if err != nil {
		log.Fatal(err.Error())
		return nil, err
	}
	return resp, nil
}

type ScanImplRPCServer struct {
	Impl ScanImpl
}

func (s *ScanImplRPCServer) ScanWithStartRowEndRow(request ScanWithStartRowEndRowRequest, resp *[]string) error {
	var err error
	*resp, err = s.Impl.ScanWithStartRowEndRow(request)
	return err
}

func (s *ScanImplRPCServer) ScanWithStartTimeEndTime(request ScanWithStartTimeEndTimeRequest, resp *[]string) error {
	var err error
	*resp, err = s.Impl.ScanWithStartTimeEndTime(request)
	return err
}

type ScanImplPlugin struct{}

func (ScanImplPlugin) Server(*gplugin.MuxBroker) (interface{}, error) {
	return &ScanImplRPCServer{}, nil
}

func (ScanImplPlugin) Client(b *gplugin.MuxBroker, c *rpc.Client) (interface{}, error) {
	return &ScanImplRPC{Client: c}, nil
}
