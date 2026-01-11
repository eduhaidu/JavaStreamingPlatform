# RTMP Authentication Testing Guide

## Overview

RTMP authentication is now enabled. Users must stream using their unique stream key.

## How to Get Your Stream Key

1. **Login** to your account at http://localhost:8080/auth.html
2. **Navigate** to Settings from your channel page
3. **Copy** your Stream Key from the settings page

## Streaming with OBS/FFmpeg

### RTMP URL Structure

```
rtmp://localhost/live/{YOUR_STREAM_KEY}
```

### OBS Studio Configuration

1. Open **Settings** → **Stream**
2. Set **Service** to `Custom...`
3. Set **Server** to: `rtmp://localhost/live`
4. Set **Stream Key** to: `YOUR_STREAM_KEY` (from settings page)
5. Click **OK** and **Start Streaming**

### FFmpeg Test Stream

```bash
# Replace YOUR_STREAM_KEY with your actual stream key
ffmpeg -re -f lavfi -i testsrc=size=1280x720:rate=30 \
  -f lavfi -i sine=frequency=1000:sample_rate=44100 \
  -c:v libx264 -preset veryfast -b:v 2500k \
  -c:a aac -b:a 128k \
  -f flv rtmp://localhost/live/YOUR_STREAM_KEY
```

## Authentication Behavior

### ✅ Valid Stream Key

- Stream is accepted
- User information is retrieved from database
- Stream is associated with user account
- Stream appears on user's channel page
- START message sent to Kafka with user info: `START|userId|username`

### ❌ Invalid Stream Key

- Stream is immediately rejected
- Connection is closed by Red5
- No stream metadata is created
- Error logged: "Rejecting stream - invalid stream key"

## Testing Scenarios

### Test 1: Valid Stream Key

1. Get your stream key from settings page
2. Start streaming with OBS or FFmpeg using your key
3. Check Red5 console for: "Stream key validated for user: {username}"
4. Visit your channel page to see the live stream

### Test 2: Invalid Stream Key

1. Try streaming with a fake key: `rtmp://localhost/live/invalid-key-12345`
2. Check Red5 console for: "Invalid stream key: invalid-key-12345"
3. Check Red5 console for: "Rejecting stream - invalid stream key"
4. Stream should fail to connect

### Test 3: Check Stream Association

1. Start a valid stream
2. Check Spring Boot console for: "Stream associated with user: {username}"
3. Query database to verify StreamMetadata has user_id set:
   ```sql
   SELECT * FROM stream_metadata WHERE stream_name = 'YOUR_STREAM_KEY';
   ```

## API Endpoints

### Validate Stream Key (used by Red5)

```bash
curl http://localhost:8080/api/stream/check-key/{streamKey}
```

**Response (Valid):**

```json
{
  "valid": true,
  "userId": 1,
  "username": "exampleuser"
}
```

**Response (Invalid):**

```json
{
  "valid": false
}
```

### Get Stream Key Validation Details

```bash
curl http://localhost:8080/api/stream/validate-key/{streamKey}
```

## Troubleshooting

### Issue: Stream connects but isn't associated with user

- **Solution**: Check Red5 logs for validation API call errors
- **Check**: Ensure Spring Boot is running on port 8080
- **Verify**: Network connectivity between Red5 and Spring Boot

### Issue: "Failed to validate stream key" in logs

- **Solution**: Verify Spring Boot API is accessible
- **Test**: `curl http://localhost:8080/api/stream/check-key/test`
- **Check**: Firewall or network issues

### Issue: Stream key not showing in settings

- **Solution**: Logout and login again
- **Check**: Database has stream_key generated for user
- **Query**: `SELECT username, stream_key FROM users WHERE username = 'yourname';`

## Monitoring

### Red5 Server Logs

```bash
tail -f /Users/eduhaidu/JavaStreamingPlatform/red5-server/log/red5.log
```

### Spring Boot Logs

Watch console output for:

- "Stream key validated for user: {username}"
- "Stream associated with user: {username} (ID: {userId})"

### Kafka Messages

Messages now include user info:

```
Key: {streamKey}
Value: START|{userId}|{username}
```

## Next Steps

1. Test with your own stream key
2. Verify stream appears on your channel page
3. Check that VOD recordings are also associated with your user
4. Consider adding stream title functionality
