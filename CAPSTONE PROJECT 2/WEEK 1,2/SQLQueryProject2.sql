-- Create database
CREATE DATABASE SmartHomeDB;
GO

USE SmartHomeDB;
GO

-- Create rooms table
CREATE TABLE rooms (
    room_id INT PRIMARY KEY IDENTITY(1,1),
    room_name VARCHAR(100) NOT NULL
);

-- Create devices table
CREATE TABLE devices (
    device_id INT PRIMARY KEY IDENTITY(1,1),
    device_name VARCHAR(100) NOT NULL,
    room_id INT NOT NULL,
    status VARCHAR(20) CHECK (status IN ('on', 'off')) NOT NULL,
    CONSTRAINT FK_devices_rooms FOREIGN KEY (room_id) REFERENCES rooms(room_id)
        ON DELETE CASCADE
);

-- Create energy_logs table with proper foreign key and cascading delete
CREATE TABLE energy_logs (
    log_id INT PRIMARY KEY IDENTITY(1,1),
    device_id INT NOT NULL,
    log_time DATETIME NOT NULL,
    energy_consumed_kwh FLOAT NOT NULL,
    CONSTRAINT FK_energy_logs_devices FOREIGN KEY (device_id)
        REFERENCES devices(device_id)
        ON DELETE CASCADE
);

-- Insert sample rooms
INSERT INTO rooms (room_name) VALUES ('Living Room'), ('Kitchen');

-- Insert sample devices
INSERT INTO devices (device_name, room_id, status)
VALUES ('Smart Light', 1, 'on'), ('Heater', 2, 'off');

-- Insert sample energy logs
INSERT INTO energy_logs (device_id, log_time, energy_consumed_kwh)
VALUES (1, '2025-06-01 08:00:00', 1.2),
       (1, '2025-06-01 09:00:00', 0.8),
       (2, '2025-06-01 08:30:00', 2.5);

CREATE PROCEDURE GetDailyEnergyUsagePerRoom
AS
BEGIN
    SELECT 
        r.room_name,
        CAST(el.log_time AS DATE) AS usage_date,
        SUM(el.energy_consumed_kwh) AS total_kwh
    FROM energy_logs el
    INNER JOIN devices d ON el.device_id = d.device_id
    INNER JOIN rooms r ON d.room_id = r.room_id
    GROUP BY r.room_name, CAST(el.log_time AS DATE)
    ORDER BY r.room_name, usage_date;
END;

EXEC GetDailyEnergyUsagePerRoom;


