# Java Streaming Platform

A distributed live streaming platform built with Java, featuring RTMP ingestion, HLS transcoding, real-time stream management, and VOD (Video on Demand) capabilities.

## 🎯 Features

- **RTMP Live Streaming**: Stream via OBS or any RTMP-compatible software
- **Stream Key Authentication**: Secure streaming with per-user stream keys
- **HLS Transcoding**: Real-time video transcoding for adaptive streaming
- **Live Stream Playback**: Watch live streams in the browser with HLS.js
- **VOD Recording**: Automatic archival of completed streams
- **User Channels**: Personal channel pages with live status and past broadcasts
- **Real-time Updates**: Kafka-based event system for stream lifecycle management
- **Object Storage**: MinIO for scalable video storage

## 🏗️ Architecture

The platform consists of four main components:

### 1. Red5 RTMP Server (Port 1935)

- Accepts RTMP streams from broadcasting software
- Validates stream keys via REST API
- Publishes stream lifecycle events to Kafka

### 2. Spring Boot Backend (Port 8080)

- REST API for authentication and stream management
- Kafka consumer for stream events
- PostgreSQL database for metadata
- JWT-based user authentication

### 3. Transcoding Worker

- Consumes stream events from Kafka
- FFmpeg-based HLS transcoding
- Uploads video segments to MinIO in real-time
- Archives completed streams to VOD storage

### 4. Frontend (Static HTML/JS)

- Modern Twitch-like UI
- Video.js player for HLS playback
- Channel pages with live streams and VOD library

## 🛠️ Technologies

### Backend

- **Java 21**: Modern Java features
- **Spring Boot 3.2.1**: REST API framework
- **Red5 Server 2.0.23**: RTMP ingestion
- **Apache Kafka 3.6.0**: Event streaming
- **PostgreSQL 14.20**: Relational database
- **MinIO 8.5.7**: S3-compatible object storage

### Media Processing

- **FFmpeg**: Video transcoding
- **HLS**: Adaptive streaming protocol

### Frontend

- **Video.js 8.6.1**: HTML5 video player
- **Vanilla JavaScript**: No framework dependencies

## 📋 Prerequisites

- Java JDK 21
- Maven 3.6+
- PostgreSQL 14+
- Apache Kafka 3.6+
- MinIO Server
- FFmpeg (with H.264 support)
- OBS Studio or similar RTMP broadcaster

## 🚀 Setup Instructions

### 1. Database Setup

```bash
# Create PostgreSQL database
psql -U postgres

CREATE DATABASE streaming_db;
CREATE USER streaming_user WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE streaming_db TO streaming_user;
```

Configure `streaming-frontend/src/main/resources/application.properties`:

```properties
spring.datasource.url=jdbc:postgresql://localhost:5432/streaming_db
spring.datasource.username=streaming_user
spring.datasource.password=your_password
```

### 2. Kafka Setup

```bash
# Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka
bin/kafka-server-start.sh config/server.properties

# Create topic
bin/kafka-topics.sh --create --topic live-stream --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

### 3. MinIO Setup

```bash
# Start MinIO server
minio server /data --console-address ":9001"

# Access MinIO Console at http://localhost:9001
# Create bucket: video-storage
# Set credentials: minioadmin / minioadmin123
```

### 4. Build Projects

```bash
# Build Red5 Plugin
cd red5-plugin
mvn clean package
cp target/red5-plugin-1.0-SNAPSHOT.jar ../red5-server/webapps/live/WEB-INF/lib/

# Build Transcoding Worker
cd ../transcoding-worker
mvn clean package

# Build Spring Boot Backend
cd ../streaming-frontend
mvn clean package
```

### 5. Configure Red5 Server

Ensure `red5-server/webapps/live/WEB-INF/lib/` contains:

- red5-plugin-1.0-SNAPSHOT.jar
- All Kafka client JARs
- OkHttp and Gson libraries

**Remove any conflicting class files:**

```bash
rm -rf red5-server/webapps/live/WEB-INF/classes
```

## 🎮 Running the Platform

### Start All Services

```bash
# Terminal 1: Red5 RTMP Server
cd red5-server
./red5.sh

# Terminal 2: Spring Boot Backend
cd streaming-frontend
mvn spring-boot:run

# Terminal 3: Transcoding Worker
cd transcoding-worker
mvn exec:java -Dexec.mainClass="org.distributed.transcoding.TranscodingWorker"
```

Or use VS Code Run configurations (included in `.vscode/launch.json`).

## 📺 Usage

### For Streamers

1. **Register an Account**

   - Navigate to http://localhost:8080/auth.html
   - Create an account

2. **Get Stream Key**

   - Go to Settings page
   - Copy your unique stream key

3. **Configure OBS**

   - **Server**: `rtmp://localhost/live`
   - **Stream Key**: Your generated key
   - **Video Bitrate**: 3000 Kbps recommended
   - **Keyframe Interval**: 2 seconds

4. **Start Streaming**
   - Click "Start Streaming" in OBS
   - Your stream appears live on the homepage

### For Viewers

1. **Browse Streams**

   - Visit http://localhost:8080
   - See all live streams in grid layout

2. **Watch Live**

   - Click any live stream card
   - Video player opens in modal

3. **Visit Channels**

   - Click on streamer's username
   - View channel page with live status and past broadcasts

4. **Watch VODs**
   - Browse past broadcasts on channel page
   - Click to watch archived streams

## 🔑 API Endpoints

### Authentication

```
POST /auth/register - Register new user
POST /auth/login    - Login and receive JWT token
```

### Streams

```
GET  /streams/previews           - Get all live streams
GET  /stream/{filename}          - Proxy HLS segments from MinIO
GET  /vod                        - Get all VOD recordings
GET  /vod/{vodPath}/{filename}   - Proxy VOD segments from MinIO
```

### Channels

```
GET  /channels/{username}        - Get user profile
GET  /channels/{username}/vods   - Get user's VOD recordings
```

### Stream Key Validation (Internal)

```
GET  /api/stream/check-key/{key} - Validate stream key (used by Red5)
```

## 📁 Project Structure

```
JavaStreamingPlatform/
├── red5-server/              # Red5 RTMP server
│   ├── webapps/live/        # Live streaming webapp
│   └── conf/                # Configuration files
├── red5-plugin/             # Custom Red5 application adapter
│   └── src/main/java/
│       └── StreamManager.java
├── transcoding-worker/      # FFmpeg transcoding service
│   └── src/main/java/
│       └── TranscodingWorker.java
├── streaming-frontend/      # Spring Boot backend + frontend
│   ├── src/main/java/       # Java backend code
│   └── src/main/resources/
│       ├── static/          # HTML/JS/CSS files
│       └── application.properties
└── README.md
```

## 🔧 Configuration Files

### application.properties

```properties
# Database
spring.datasource.url=jdbc:postgresql://localhost:5432/streaming_db
spring.jpa.hibernate.ddl-auto=update

# Kafka
spring.kafka.bootstrap-servers=localhost:9092
spring.kafka.consumer.group-id=streaming-group

# Server
server.port=8080

# MinIO (used by StreamController)
minio.url=http://localhost:9000
minio.access-key=minioadmin
minio.secret-key=minioadmin123
minio.bucket-name=video-storage
```

### FFmpeg Transcoding Settings

```bash
# HLS Settings (in TranscodingWorker.java)
-codec:v libx264          # H.264 video codec
-preset veryfast          # Encoding speed
-b:v 3000k               # Video bitrate
-codec:a aac             # AAC audio codec
-b:a 128k                # Audio bitrate
-hls_time 4              # Segment duration (4 seconds)
-hls_list_size 5         # Playlist window size
-hls_flags delete_segments # Auto-cleanup old segments
```

## 🔐 Security Features

- **JWT Authentication**: Secure API endpoints
- **Stream Key Validation**: Per-user authentication for RTMP streams
- **CORS Configuration**: Configurable cross-origin policies
- **Password Encryption**: BCrypt hashing for user passwords

## 🎥 Video Storage Structure

### MinIO Bucket Layout

```
video-storage/
├── {streamKey}.m3u8          # Live HLS playlist
├── {streamKey}*.ts           # Live video segments
└── vod/
    └── {streamKey}_{timestamp}/
        ├── {streamKey}.m3u8  # VOD playlist
        └── {streamKey}*.ts   # VOD segments
```

## 📊 Database Schema

### users

- id (Primary Key)
- username (Unique)
- password (BCrypt hash)
- email
- stream_key (UUID)
- created_at

### stream_metadata

- stream_name (Primary Key)
- user_id (Foreign Key → users)
- is_live
- start_time
- end_time
- stream_title
- viewer_count

### vod_recordings

- id (Primary Key)
- stream_name
- vod_path
- user_id (Foreign Key → users)
- recorded_at
- duration (seconds)
- file_size (bytes)

## 🔄 Event Flow

### Stream Start

1. OBS connects to Red5 with stream key
2. Red5 validates key via Spring Boot API
3. Red5 publishes START event to Kafka with user info
4. Spring Boot creates stream_metadata record
5. TranscodingWorker starts FFmpeg transcoding
6. HLS segments uploaded to MinIO in real-time
7. Stream appears live on homepage

### Stream Stop

1. OBS disconnects from Red5
2. Red5 publishes STOP event to Kafka
3. Spring Boot marks stream as ended
4. TranscodingWorker stops FFmpeg
5. All segments archived to vod/ folder
6. Live segments deleted from MinIO
7. ARCHIVE event creates vod_recordings entry
8. VOD appears in Past Broadcasts

## 🐛 Troubleshooting

### Stream Not Appearing Live

- Check Red5 logs: `tail -f /tmp/red5.log`
- Verify Kafka consumer is running
- Check Spring Boot logs for Kafka messages

### Video Not Playing

- Verify MinIO is running and accessible
- Check browser console for HLS errors
- Ensure FFmpeg is transcoding (check TranscodingWorker logs)

### Authentication Issues

- Clear browser cache and cookies
- Verify JWT token in localStorage
- Check Spring Security configuration

### Red5 Plugin Not Loading

- Remove `WEB-INF/classes` directory if present
- Verify JAR is in `WEB-INF/lib/`
- Check for dependency conflicts

## 📈 Performance Tuning

### FFmpeg Optimization

- Adjust preset: `ultrafast` for low latency, `veryfast` for quality
- Tune segment duration: 2-4 seconds recommended
- Configure buffer size based on network conditions

### Kafka Configuration

- Increase partitions for higher throughput
- Tune consumer poll interval
- Configure retention policies

### MinIO Optimization

- Enable erasure coding for redundancy
- Configure lifecycle policies for old VODs
- Use CDN for global distribution

## 🤝 Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License.

## 👥 Authors

- Built with ❤️ as a distributed systems project

## 🙏 Acknowledgments

- Red5 Server for RTMP ingestion
- FFmpeg for video transcoding
- Apache Kafka for event streaming
- Video.js for HTML5 video playback
- Spring Boot for backend framework

## 📞 Support

For issues and questions:

- Check the troubleshooting section
- Review Red5/Spring Boot logs
- Ensure all services are running

---

**Happy Streaming! 🎬**
