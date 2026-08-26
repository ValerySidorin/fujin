package proto

import "testing"

func TestClientDescriptorsUseCanonicalServerNamespace(t *testing.T) {
	if got, want := string(File_fujin_proto.Package()), "fujin.v1"; got != want {
		t.Fatalf("protobuf package = %q, want %q", got, want)
	}
	if got, want := FujinService_Stream_FullMethodName, "/fujin.v1.FujinService/Stream"; got != want {
		t.Fatalf("gRPC method path = %q, want %q", got, want)
	}
}
