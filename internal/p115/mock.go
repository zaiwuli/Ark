package p115

import (
	"context"
	"sync"
)

// MockClient is a deterministic in-memory Port for unit and integration tests.
type MockClient struct {
	mu               sync.Mutex
	Folders          map[string]string
	folderIDs        map[string]bool
	Listings         map[string][]CloudFile
	Paths            map[string][]string
	Submissions      [][]string
	SubmitErr        error
	Snapshots        []OfflineTask
	Deleted          []string
	Moved            []string
	Life             []LifeEvent
	PrepareStatuses  []string // optional sequence for testing retry / password / failure
	PrepareCallCount int
	ExtractStatuses  []string // optional sequence for testing cloud extract progress / failure
	ExtractCallCount int
}

func NewMockClient() *MockClient {
	return &MockClient{
		Folders:   make(map[string]string),
		folderIDs: make(map[string]bool),
		Listings:  make(map[string][]CloudFile),
		Paths:     make(map[string][]string),
	}
}

func (m *MockClient) Files(_ context.Context, cid string, offset int) (FilePage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if offset > 0 {
		return FilePage{}, nil
	}
	items := append([]CloudFile(nil), m.Listings[cid]...)
	return FilePage{Files: items}, nil
}

func (m *MockClient) DirectoryPath(_ context.Context, cid string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if p, ok := m.Paths[cid]; ok {
		return append([]string(nil), p...), nil
	}
	return []string{"0", cid}, nil
}

func (m *MockClient) EnsureFolder(_ context.Context, parentCID, name string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := parentCID + "/" + name
	if id, ok := m.Folders[key]; ok {
		return id, nil
	}
	id := "cid_" + name
	if m.folderIDs[id] {
		id = "cid_" + parentCID + "_" + name
	}
	m.folderIDs[id] = true
	m.Folders[key] = id
	m.Paths[id] = []string{"0", parentCID, id}
	return id, nil
}

func (m *MockClient) MoveVerified(_ context.Context, loc FileLocator, targetCID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Moved = append(m.Moved, loc.FID+"->"+targetCID)
	if list, ok := m.Listings[loc.CID]; ok {
		filtered := list[:0]
		for _, f := range list {
			if f.FID != loc.FID {
				filtered = append(filtered, f)
			}
		}
		m.Listings[loc.CID] = filtered
	}
	return nil
}

func (m *MockClient) DeleteVerified(_ context.Context, loc FileLocator) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Deleted = append(m.Deleted, loc.FID)
	if list, ok := m.Listings[loc.CID]; ok {
		filtered := list[:0]
		for _, f := range list {
			if f.FID != loc.FID {
				filtered = append(filtered, f)
			}
		}
		m.Listings[loc.CID] = filtered
	}
	return nil
}

func (m *MockClient) SubmitOffline(_ context.Context, links []string, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.SubmitErr != nil {
		return m.SubmitErr
	}
	cp := append([]string(nil), links...)
	m.Submissions = append(m.Submissions, cp)
	return nil
}

func (m *MockClient) OfflineSnapshot(_ context.Context) ([]OfflineTask, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]OfflineTask(nil), m.Snapshots...), nil
}

func (m *MockClient) PrepareExtract(_ context.Context, _, _ string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	idx := m.PrepareCallCount
	m.PrepareCallCount++
	if idx < len(m.PrepareStatuses) {
		return m.PrepareStatuses[idx], nil
	}
	return "success", nil
}
func (m *MockClient) InspectExtract(_ context.Context, _ string) (string, error) {
	return "success", nil
}
func (m *MockClient) StartExtract(_ context.Context, _, _ string) (string, error) {
	return "ext_1", nil
}
func (m *MockClient) ExtractStatus(_ context.Context, _ string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	idx := m.ExtractCallCount
	m.ExtractCallCount++
	if idx < len(m.ExtractStatuses) {
		return m.ExtractStatuses[idx], nil
	}
	return "success", nil
}
func (m *MockClient) ArchiveManifest(_ context.Context, _ string) ([]ArchiveEntry, error) {
	return nil, nil
}
func (m *MockClient) LifeEvents(_ context.Context, _ int64) ([]LifeEvent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]LifeEvent(nil), m.Life...), nil
}
func (m *MockClient) DownloadURL(_ context.Context, _ string) (DownloadInfo, error) {
	return DownloadInfo{URL: "https://cdn.115.com/mock"}, nil
}
