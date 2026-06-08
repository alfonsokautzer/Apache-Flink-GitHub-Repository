package main

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// CompletedCheckpoint represents a completed checkpoint with an ID.
type CompletedCheckpoint struct {
	ID int64
}

// CompletedCheckpointStore defines the interface for storing completed checkpoints.
type CompletedCheckpointStore interface {
	AddCheckpoint(checkpoint CompletedCheckpoint) error
	GetLatestCheckpoint() (CompletedCheckpoint, error)
	GetAllCheckpoints() ([]CompletedCheckpoint, error)
}

// MockCompletedCheckpointStore simulates a store (like ZooKeeper/Kubernetes) with latency.
type MockCompletedCheckpointStore struct {
	mu          sync.Mutex
	checkpoints []CompletedCheckpoint
	latency     time.Duration
}

func NewMockCompletedCheckpointStore(latency time.Duration) *MockCompletedCheckpointStore {
	return &MockCompletedCheckpointStore{
		checkpoints: make([]CompletedCheckpoint, 0),
		latency:     latency,
	}
}

func (s *MockCompletedCheckpointStore) AddCheckpoint(checkpoint CompletedCheckpoint) error {
	// Simulate network latency for metadata write
	time.Sleep(s.latency)
	s.mu.Lock()
	defer s.mu.Unlock()

	// Ensure atomic update and pruning
	s.checkpoints = append(s.checkpoints, checkpoint)
	return nil
}

func (s *MockCompletedCheckpointStore) GetLatestCheckpoint() (CompletedCheckpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.checkpoints) == 0 {
		return CompletedCheckpoint{}, errors.New("no checkpoints available")
	}
	return s.checkpoints[len(s.checkpoints)-1], nil
}

func (s *MockCompletedCheckpointStore) GetAllCheckpoints() ([]CompletedCheckpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	copied := make([]CompletedCheckpoint, len(s.checkpoints))
	copy(copied, s.checkpoints)
	return copied, nil
}

// CheckpointCoordinator coordinates checkpointing and recovery.
type CheckpointCoordinator struct {
	mu                           sync.Mutex
	store                        CompletedCheckpointStore
	highestCompletedCheckpointID int64
	currentEpoch                 int64
}

func NewCheckpointCoordinator(store CompletedCheckpointStore) *CheckpointCoordinator {
	return &CheckpointCoordinator{
		store: store,
	}
}

// CompleteCheckpoint registers a completed checkpoint.
func (c *CheckpointCoordinator) CompleteCheckpoint(id int64) error {
	c.mu.Lock()
	if id > c.highestCompletedCheckpointID {
		c.highestCompletedCheckpointID = id
	}
	c.mu.Unlock()

	checkpoint := CompletedCheckpoint{ID: id}
	return c.store.AddCheckpoint(checkpoint)
}

// RestoreLatestCheckpointedState restores the state, enforcing monotonic checkpoint ID validation.
func (c *CheckpointCoordinator) RestoreLatestCheckpointedState() error {
	c.mu.Lock()
	expectedMinID := c.highestCompletedCheckpointID
	epoch := c.currentEpoch
	c.mu.Unlock()

	latest, err := c.store.GetLatestCheckpoint()
	if err != nil {
		return fmt.Errorf("failed to retrieve latest checkpoint: %w", err)
	}

	// Stale State Rejection: If the retrieved checkpoint ID is lower than the highest known completed ID, reject it.
	if latest.ID < expectedMinID {
		return fmt.Errorf("stale state recovery rejected: retrieved checkpoint ID %d is older than the highest completed checkpoint ID %d in epoch %d", latest.ID, expectedMinID, epoch)
	}

	fmt.Printf("Successfully restored state to checkpoint %d\n", latest.ID)
	return nil
}

// Failover increments the epoch/generation counter.
func (c *CheckpointCoordinator) Failover() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.currentEpoch++
	fmt.Printf("Failover triggered. Current epoch: %d\n", c.currentEpoch)
}

func main() {
	fmt.Println("Starting Checkpoint Coordinator Race Condition Simulation...")

	// Create a store with 100ms write latency to simulate ZooKeeper/Kubernetes delay
	store := NewMockCompletedCheckpointStore(100 * time.Millisecond)
	coordinator := NewCheckpointCoordinator(store)

	// 1. Complete Checkpoint 1
	fmt.Println("Completing Checkpoint 1...")
	_ = coordinator.CompleteCheckpoint(1)

	// 2. Trigger failover and start recovery
	coordinator.Failover()
	_ = coordinator.RestoreLatestCheckpointedState()

	// 3. Complete Checkpoint 2 asynchronously (simulating delayed write)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		fmt.Println("Completing Checkpoint 2 (delayed write)...")
		_ = coordinator.CompleteCheckpoint(2)
	}()

	// Wait a tiny bit so the coordinator registers the completion in-memory, but the store write is still in progress
	time.Sleep(10 * time.Millisecond)

	// 4. Induce a second failover immediately while the metadata write for Checkpoint 2 is still propagating
	coordinator.Failover()

	// 5. Attempt recovery. The store might still return Checkpoint 1 because Checkpoint 2 write is delayed.
	fmt.Println("Attempting recovery during rapid failover...")
	err := coordinator.RestoreLatestCheckpointedState()
	if err != nil {
		fmt.Printf("Expected rejection occurred: %v\n", err)
	} else {
		fmt.Println("WARNING: Restored stale state!")
	}

	// Wait for the delayed write to finish
	wg.Wait()

	// 6. Retry recovery now that the write has completed
	fmt.Println("Retrying recovery after write completes...")
	err = coordinator.RestoreLatestCheckpointedState()
	if err != nil {
		fmt.Printf("Recovery failed: %v\n", err)
	} else {
		fmt.Println("Recovery succeeded after write completed.")
	}
}
