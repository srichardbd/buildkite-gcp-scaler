package bk

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	buildkite "github.com/buildkite/go-buildkite/v3/buildkite"
	hclog "github.com/hashicorp/go-hclog"
)

type Client struct {
	AgentToken  string
	Org         string
	IdleTimeout *time.Duration
	QueueName   string
	BkClient    *buildkite.Client
	Logger      hclog.Logger
}

func NewClient(agentToken string, org string, idleTimeout *time.Duration, queueName string, logger hclog.Logger) *Client {
	client, err := buildkite.NewOpts(buildkite.WithTokenAuth(agentToken))
	if err != nil {
		log.Fatalf("creating buildkite API client failed: %v", err)
	}
	return &Client{
		Org:         org,
		IdleTimeout: idleTimeout,
		QueueName:   queueName,
		BkClient:    client,
		AgentToken:  agentToken,
		Logger:      logger.Named("bkclient"),
	}
}

// stopAgents
func (c *Client) StopAgents(ctx context.Context, agentNames []string) error {
	agents, _, err := c.BkClient.Agents.List(c.Org, nil)

	if err != nil {
		c.Logger.Error(fmt.Sprint("list agents failed: %s", err))
		return err
	}

	for _, agent := range agents {
		// Dereference agent name and ID
		agentName := *agent.Name
		agentId := *agent.ID
		if contains(agentNames, agentName) {
			c.Logger.Debug(fmt.Sprint("stopping agent: %s", agentName))
			_, err := c.BkClient.Agents.Stop(c.Org, agentId, true)
			if err != nil {
				c.Logger.Error(fmt.Sprint("stop agent failed: %s", err))
				return err
			}
		}
	}

	return nil
}

// Returns a list of idle agent names
func (c *Client) GetIdleAgents(ctx context.Context) ([]string, error) {
	agents, _, err := c.BkClient.Agents.List(c.Org, nil)

	if err != nil {
		c.Logger.Error(fmt.Sprint("list agents failed: %s", err))
		return []string{}, err
	}

	data, err := json.MarshalIndent(agents, "", "\t")

	if err != nil {
		c.Logger.Error(fmt.Sprint("json encode failed: %s", err))
		return []string{}, err
	}

	c.Logger.Debug(fmt.Sprintf("%s", string(data)))

	// Get current time
	now := time.Now().UTC()

	idleAgents := []string{}
	for _, agent := range agents {
		// Dereference agent name
		agentName := *agent.Name
		// filter agents via meta-data queue name
		if agent.Metadata != nil && containsQueue(agent.Metadata, c.QueueName) {
			if agent.Job == nil {
				// If there's no job, check last_job_finished_at
				if agent.LastJobFinishedAt != nil {
					lastJobFinishedAt := agent.LastJobFinishedAt.Time // Access the embedded time.Time directly
					// Check if the agent has been idle longer than IdleTimeout
					if now.Sub(lastJobFinishedAt) > *c.IdleTimeout {
						c.Logger.Debug(fmt.Sprintf("Agent %s has been idle longer than %s, since %s\n", agentName, c.IdleTimeout, agent.LastJobFinishedAt.String()))
						idleAgents = append(idleAgents, agentName)
					}
				}
			}
		}
	}
	return idleAgents, err
}

// containsQueue checks if the queueName is present in the agent's meta_data
func containsQueue(metaData []string, queueName string) bool {
	for _, data := range metaData {
		if strings.Contains(data, "queue="+queueName) {
			return true
		}
	}
	return false
}

// contains checks if the target string is in the list of strings.
func contains(list []string, target string) bool {
	for _, item := range list {
		if item == target {
			return true
		}
	}
	return false
}
