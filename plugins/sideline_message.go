package plugins

type SidelineMessage struct {
	GroupId           string
	Partition         int32
	EntityId          string
	Offset            int64
	ConsumerGroupName string
	ClusterName       string
	Message           []byte
	Version           int32
	ConnectionType    string
	SidelineMeta      []byte
}
