
---

### 2. MariaDB/MySQL Table Creation Script (`sql/create_tables.sql`)

Place this file in a directory named `sql` at the root of your repository. Using a dedicated `sql` folder for database scripts is a common and acceptable practice.

```sql
-- create_tables.sql
-- This script creates the necessary tables for DMParser if they do not already exist.
-- Run this script on your MariaDB/MySQL database.

-- Table: campaigns
CREATE TABLE IF NOT EXISTS campaigns (
    id INT AUTO_INCREMENT PRIMARY KEY,
    campaignName VARCHAR(255) NOT NULL,
    vertical INT NOT NULL DEFAULT 1,
    textingActive TINYINT NOT NULL DEFAULT 0,
    flag INT NOT NULL,
    emoji VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Table: emoji
CREATE TABLE IF NOT EXISTS emoji (
    id INT AUTO_INCREMENT PRIMARY KEY,
    e VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Table: address
CREATE TABLE IF NOT EXISTS address (
    id INT AUTO_INCREMENT PRIMARY KEY,
    street VARCHAR(255),
    unit_type VARCHAR(50),
    unit_num VARCHAR(50),
    mail_city VARCHAR(100),
    state VARCHAR(50),
    zip VARCHAR(20),
    latitude VARCHAR(50),
    longitude VARCHAR(50),
    fullname VARCHAR(255),
    fname VARCHAR(100),
    lname VARCHAR(100),
    mailingAddress VARCHAR(255),
    mailingCity VARCHAR(100),
    mailingState VARCHAR(50),
    mailingZip VARCHAR(20),
    flag INT,
    DMID VARCHAR(100),
    via INT,
    map_image_url VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_dmid (DMID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Table: phonequeue
CREATE TABLE IF NOT EXISTS phonequeue (
    id INT AUTO_INCREMENT PRIMARY KEY,
    aid INT,
    phone1 VARCHAR(50),
    phone2 VARCHAR(50),
    phone3 VARCHAR(50),
    step INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (aid) REFERENCES address(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
