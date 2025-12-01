CREATE TABLE sensor_readings (
    id BIGSERIAL PRIMARY KEY,
    sensor_id VARCHAR(100) NOT NULL,
    sensor_type VARCHAR(50) NOT NULL,
    building VARCHAR(100),
    floor INTEGER,
    room VARCHAR(100),
    timestamp TIMESTAMP NOT NULL,
    value NUMERIC(10, 2) NOT NULL,
    unit VARCHAR(20),
    battery_level INTEGER,
    signal_strength INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_sensor_timestamp (sensor_id, timestamp),
    INDEX idx_sensor_type (sensor_type)
);

CREATE TABLE alerts (
    id BIGSERIAL PRIMARY KEY,
    sensor_id VARCHAR(100) NOT NULL,
    alert_type VARCHAR(50) NOT NULL,
    severity VARCHAR(20) NOT NULL, -- critical, warning, info
    threshold_value NUMERIC(10, 2),
    actual_value NUMERIC(10, 2),
    message TEXT,
    triggered_at TIMESTAMP NOT NULL,
    resolved_at TIMESTAMP,
    status VARCHAR(20) DEFAULT 'active', -- active, resolved, acknowledged
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE aggregated_stats (
    id BIGSERIAL PRIMARY KEY,
    sensor_id VARCHAR(100) NOT NULL,
    sensor_type VARCHAR(50) NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    avg_value NUMERIC(10, 2),
    min_value NUMERIC(10, 2),
    max_value NUMERIC(10, 2),
    count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(sensor_id, window_start)
);