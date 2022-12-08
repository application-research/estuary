package collections

import (
	"github.com/application-research/estuary/util"
	"testing"
)

const (
	name        = "a"
	contentPath = "/dir1/dir2/dir3/a"
	contentId   = uint(1)
	coluuid     = "collection uuid"
)

func setupRef() util.ContentWithPath {
	return util.ContentWithPath{
		Path: contentPath,
		Content: util.Content{
			ID:   contentId,
			Name: name,
		},
	}
}

func TestSubDirectory(t *testing.T) {
	ref := setupRef()
	queryDir := "/dir1"
	expectedResponse := CollectionListResponse{
		Name:    "dir2",
		Type:    CidTypeDir,
		Size:    0,
		ContID:  0,
		Dir:     queryDir,
		ColUuid: coluuid,
	}
	runTest(t, ref, queryDir, coluuid, expectedResponse)
}

func TestSubDirectory2(t *testing.T) {
	ref := setupRef()
	queryDir := "/dir1/dir2"
	expectedResponse := CollectionListResponse{
		Name:    "dir3",
		Type:    CidTypeDir,
		Size:    0,
		ContID:  0,
		Dir:     queryDir,
		ColUuid: coluuid,
	}
	runTest(t, ref, queryDir, coluuid, expectedResponse)
}

func TestSubDirectoryWithFileInIt(t *testing.T) {
	ref := setupRef()
	queryDir := "/dir1/dir2/dir3"
	expectedResponse := CollectionListResponse{
		Name:    name,
		Type:    CidTypeFile,
		Size:    0,
		ContID:  contentId,
		Dir:     queryDir,
		ColUuid: coluuid,
	}
	runTest(t, ref, queryDir, coluuid, expectedResponse)
}

func TestFullPath(t *testing.T) {
	ref := setupRef()
	queryDir := "/dir1/dir2/dir3/a"
	expectedResponse := CollectionListResponse{
		Name:    name,
		Type:    CidTypeFile,
		Size:    0,
		ContID:  contentId,
		Dir:     queryDir,
		ColUuid: coluuid,
	}
	runTest(t, ref, queryDir, coluuid, expectedResponse)
}

func TestInvalidDirectory(t *testing.T) {
	ref := setupRef()
	queryDir := "/dir0"
	expectedResponse := CollectionListResponse{}
	runTest(t, ref, queryDir, coluuid, expectedResponse)

}

func runTest(t *testing.T, ref util.ContentWithPath, queryDir, coluuid string, expectedResponse CollectionListResponse) {
	dirs := make(map[string]bool)
	response, _, err := getDirectoryContent(ref, dirs, queryDir, coluuid)

	if err != nil {
		t.Fatalf("Error: %v\n", err)
	}

	parsedResponse := CollectionListResponse{
		Name:    response.Name,
		Type:    response.Type,
		Size:    response.Size,
		ContID:  response.ContID,
		Dir:     response.Dir,
		ColUuid: response.ColUuid,
	}

	if parsedResponse != expectedResponse {
		t.Fatalf("Expected %v, got %v, %v\n", expectedResponse, response, response.Cid.CID)
	}
}
