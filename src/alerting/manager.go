package alerting

import (
	"fmt"
	"log"
	"net/smtp"
	"strings"
	"sync"
	"time"

	"telemy/config"
	"telemy/query"
	"telemy/storage"
)

// Manager manages the alerting functionality
type Manager struct {
	config         config.AlertsConfig
	storageManager *storage.Manager
	queryEngine    *query.Engine
	rules          []*AlertRule
	stopChan       chan struct{}
	wg             sync.WaitGroup
	mu             sync.RWMutex
	running        bool
}

// AlertRule represents an alert rule
type AlertRule struct {
	Name       string
	DataSource string
	Query      string
	Duration   time.Duration
	Severity   string
	lastFired  time.Time
	active     bool
}

// AlertEvent represents an alert event
type AlertEvent struct {
	Rule      *AlertRule
	Timestamp time.Time
	Details   string
}

// NewManager creates a new alerting manager
func NewManager(cfg config.AlertsConfig, storageManager *storage.Manager) (*Manager, error) {
	// Create query engine
	queryEngine, err := query.NewEngine(storageManager)
	if err != nil {
		return nil, fmt.Errorf("failed to create query engine: %w", err)
	}

	// Create manager
	manager := &Manager{
		config:         cfg,
		storageManager: storageManager,
		queryEngine:    queryEngine,
		stopChan:       make(chan struct{}),
	}

	// Parse alert rules
	if err := manager.parseRules(); err != nil {
		return nil, fmt.Errorf("failed to parse alert rules: %w", err)
	}

	return manager, nil
}

// Start starts the alerting manager
func (m *Manager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.running {
		return nil
	}

	// Start alert checker
	m.wg.Add(1)
	go m.checkAlerts()

	m.running = true
	log.Println("Alerting manager started")
	return nil
}

// Stop stops the alerting manager
func (m *Manager) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.running {
		return nil
	}

	// Stop alert checker
	close(m.stopChan)
	m.wg.Wait()

	m.running = false
	log.Println("Alerting manager stopped")
	return nil
}

// parseRules parses the alert rules from the configuration
func (m *Manager) parseRules() error {
	m.rules = make([]*AlertRule, 0, len(m.config.Rules))

	for _, ruleConfig := range m.config.Rules {
		// Parse duration
		duration, err := time.ParseDuration(ruleConfig.Duration)
		if err != nil {
			return fmt.Errorf("invalid duration for rule %s: %w", ruleConfig.Name, err)
		}

		// Create rule
		rule := &AlertRule{
			Name:       ruleConfig.Name,
			DataSource: ruleConfig.DataSource,
			Query:      ruleConfig.Query,
			Duration:   duration,
			Severity:   ruleConfig.Severity,
		}

		m.rules = append(m.rules, rule)
	}

	return nil
}

// checkAlerts periodically checks alert rules
func (m *Manager) checkAlerts() {
	defer m.wg.Done()

	// Check alerts every minute
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.evaluateRules()
		case <-m.stopChan:
			return
		}
	}
}

// evaluateRules evaluates all alert rules
func (m *Manager) evaluateRules() {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, rule := range m.rules {
		// Evaluate the rule
		triggered, details, err := m.evaluateRule(rule)
		if err != nil {
			log.Printf("Error evaluating rule %s: %v", rule.Name, err)
			continue
		}

		// Check if the rule is triggered
		if triggered {
			// Check if the rule was already active
			if !rule.active {
				// Rule is newly triggered
				rule.active = true
				rule.lastFired = time.Now()

				// Create alert event
				event := &AlertEvent{
					Rule:      rule,
					Timestamp: time.Now(),
					Details:   details,
				}

				// Send alert
				if err := m.sendAlert(event); err != nil {
					log.Printf("Error sending alert for rule %s: %v", rule.Name, err)
				}
			}
		} else {
			// Rule is not triggered
			rule.active = false
		}
	}
}

// evaluateRule evaluates a single alert rule
func (m *Manager) evaluateRule(rule *AlertRule) (bool, string, error) {
	// Calculate time range
	endTime := time.Now()
	startTime := endTime.Add(-rule.Duration)

	// Execute query based on data source
	var result *query.QueryResult
	var err error
	switch rule.DataSource {
	case "metrics":
		result, err = m.queryEngine.QueryMetrics(rule.Query, startTime.Format(time.RFC3339), endTime.Format(time.RFC3339), "")
	case "logs":
		result, err = m.queryEngine.QueryLogs(rule.Query, startTime.Format(time.RFC3339), endTime.Format(time.RFC3339), "100")
	case "traces":
		result, err = m.queryEngine.QueryTraces(rule.Query, startTime.Format(time.RFC3339), endTime.Format(time.RFC3339), "100")
	default:
		return false, "", fmt.Errorf("unsupported data source: %s", rule.DataSource)
	}

	if err != nil {
		return false, "", fmt.Errorf("error executing query: %w", err)
	}

	// Check if the rule is triggered
	triggered, details := m.checkResult(rule, result)
	return triggered, details, nil
}

// checkResult checks if the query result triggers the alert rule
func (m *Manager) checkResult(rule *AlertRule, result *query.QueryResult) (bool, string) {
	// Check based on data source
	switch rule.DataSource {
	case "metrics":
		// Check if any metric value exceeds the threshold
		metricsResult, ok := result.Data.(query.MetricsQueryResult)
		if !ok {
			return false, "invalid metrics result"
		}

		// Check if there are any results
		if len(metricsResult.Result) == 0 {
			return false, "no metrics found"
		}

		// Check each series
		for _, series := range metricsResult.Result {
			// Check if there are any values
			if len(series.Values) == 0 {
				continue
			}

			// Check the latest value
			latestValue := series.Values[len(series.Values)-1][1].(float64)
			threshold := 0.0

			// Parse the query to extract the threshold
			// This is a simple implementation that assumes the query is in the form "metric > threshold"
			parts := strings.Split(rule.Query, ">")
			if len(parts) == 2 {
				fmt.Sscanf(strings.TrimSpace(parts[1]), "%f", &threshold)
			}

			// Check if the value exceeds the threshold
			if latestValue > threshold {
				details := fmt.Sprintf("Metric value %.2f exceeds threshold %.2f", latestValue, threshold)
				return true, details
			}
		}

	case "logs":
		// Check if there are any logs matching the query
		logsResult, ok := result.Data.(query.LogsQueryResult)
		if !ok {
			return false, "invalid logs result"
		}

		// Check if there are any logs
		if len(logsResult.Logs) > 0 {
			details := fmt.Sprintf("Found %d logs matching the query", len(logsResult.Logs))
			return true, details
		}

	case "traces":
		// Check if there are any traces matching the query
		tracesResult, ok := result.Data.(query.TracesQueryResult)
		if !ok {
			return false, "invalid traces result"
		}

		// Check if there are any traces
		if len(tracesResult.Traces) > 0 {
			details := fmt.Sprintf("Found %d traces matching the query", len(tracesResult.Traces))
			return true, details
		}
	}

	return false, ""
}

// sendAlert sends an alert notification
func (m *Manager) sendAlert(event *AlertEvent) error {
	// Check if email alerts are enabled
	if !m.config.Email.Enabled {
		// Just log the alert
		log.Printf("Alert: %s - %s", event.Rule.Name, event.Details)
		return nil
	}

	// Get email template
	template, ok := m.config.Email.Templates["default"]
	if !ok {
		template = "Alert: {alertName} triggered at {timestamp}. Details: {details}"
	}

	// Replace placeholders in the template
	subject := fmt.Sprintf("Alert: %s", event.Rule.Name)
	body := template
	body = strings.ReplaceAll(body, "{alertName}", event.Rule.Name)
	body = strings.ReplaceAll(body, "{timestamp}", event.Timestamp.Format(time.RFC3339))
	body = strings.ReplaceAll(body, "{details}", event.Details)
	body = strings.ReplaceAll(body, "{severity}", event.Rule.Severity)

	// Send email
	return m.sendEmail(subject, body)
}

// sendEmail sends an email notification
func (m *Manager) sendEmail(subject, body string) error {
	// Prepare email
	from := m.config.Email.FromAddress
	to := m.config.Email.ToAddresses

	// Prepare message
	message := []byte(fmt.Sprintf("From: %s\r\n", from) +
		fmt.Sprintf("To: %s\r\n", strings.Join(to, ", ")) +
		fmt.Sprintf("Subject: %s\r\n", subject) +
		"\r\n" +
		body)

	// Connect to SMTP server
	auth := smtp.PlainAuth("", m.config.Email.Username, m.config.Email.Password, m.config.Email.SMTPServer)
	addr := fmt.Sprintf("%s:%d", m.config.Email.SMTPServer, m.config.Email.SMTPPort)

	// Send email
	err := smtp.SendMail(addr, auth, from, to, message)
	if err != nil {
		return fmt.Errorf("error sending email: %w", err)
	}

	log.Printf("Alert email sent: %s", subject)
	return nil
}
