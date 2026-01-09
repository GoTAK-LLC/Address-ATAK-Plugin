package com.gotak.address.search;

import android.content.Context;
import android.os.Handler;
import android.os.Looper;

import com.atakmap.coremap.log.Log;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manages downloading and updating offline address databases.
 * Downloads pre-built SQLite databases from GitHub releases.
 */
public class OfflineDataManager {
    private static final String TAG = "OfflineDataManager";
    
    // GitHub repository hosting the address databases
    // Downloads from GitHub Releases (supports large files)
    private static final String GITHUB_REPO = "GoTAK-LLC/Address-ATAK-Plugin-";
    private static final String RELEASE_TAG = "databases";
    private static final String BASE_URL = "https://github.com/" + GITHUB_REPO + "/releases/download/" + RELEASE_TAG + "/";
    private static final String MANIFEST_URL = BASE_URL + "manifest.json";
    
    private static final String USER_AGENT = "ATAK-AddressPlugin/1.0";
    private static final int CONNECT_TIMEOUT = 15000;
    private static final int READ_TIMEOUT = 60000; // Longer timeout for large files
    private static final int BUFFER_SIZE = 8192;
    
    private final Context context;
    private final OfflineAddressDatabase database;
    private final ExecutorService executor;
    private final Handler mainHandler;
    
    // Currently downloading state
    private final AtomicBoolean isDownloading = new AtomicBoolean(false);
    private final AtomicBoolean cancelRequested = new AtomicBoolean(false);
    
    public OfflineDataManager(Context context, OfflineAddressDatabase database) {
        this.context = context;
        this.database = database;
        this.executor = Executors.newSingleThreadExecutor();
        this.mainHandler = new Handler(Looper.getMainLooper());
    }
    
    /**
     * State information from manifest.
     */
    public static class StateInfo {
        public String id;           // e.g., "virginia"
        public String name;         // e.g., "Virginia"
        public String abbrev;       // e.g., "VA"
        public long size;           // File size in bytes
        public int placeCount;      // Number of searchable places
        public String filename;     // e.g., "virginia.db"
        public boolean downloaded;  // Whether already downloaded locally
        
        public String getSizeFormatted() {
            if (size < 1024) {
                return size + " B";
            } else if (size < 1024 * 1024) {
                return String.format("%.1f KB", size / 1024.0);
            } else {
                return String.format("%.1f MB", size / (1024.0 * 1024.0));
            }
        }
        
        public String getPlaceCountFormatted() {
            if (placeCount >= 1000000) {
                return String.format("%.1fM places", placeCount / 1000000.0);
            } else if (placeCount >= 1000) {
                return String.format("%.0fK places", placeCount / 1000.0);
            }
            return placeCount + " places";
        }
    }
    
    /**
     * Callback for fetching available states.
     */
    public interface ManifestCallback {
        void onSuccess(List<StateInfo> states);
        void onError(String error);
    }
    
    /**
     * Callback for download progress.
     */
    public interface DownloadCallback {
        void onProgress(int percent, long downloadedBytes, long totalBytes);
        void onComplete(File dbFile);
        void onError(String error);
        void onCancelled();
    }
    
    /**
     * Fetch the manifest of available states.
     */
    public void fetchAvailableStates(ManifestCallback callback) {
        executor.execute(() -> {
            try {
                List<StateInfo> states = downloadManifest();
                
                // Mark which states are already downloaded
                List<String> downloaded = database.getDownloadedStates();
                for (StateInfo state : states) {
                    state.downloaded = downloaded.contains(state.id);
                }
                
                mainHandler.post(() -> callback.onSuccess(states));
            } catch (Exception e) {
                Log.e(TAG, "Failed to fetch manifest: " + e.getMessage(), e);
                mainHandler.post(() -> callback.onError(e.getMessage()));
            }
        });
    }
    
    /**
     * Download manifest.json from the server.
     */
    private List<StateInfo> downloadManifest() throws IOException, JSONException {
        HttpURLConnection connection = null;
        try {
            URL url = new URL(MANIFEST_URL);
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setRequestProperty("User-Agent", USER_AGENT);
            connection.setConnectTimeout(CONNECT_TIMEOUT);
            connection.setReadTimeout(READ_TIMEOUT);
            
            int responseCode = connection.getResponseCode();
            if (responseCode != HttpURLConnection.HTTP_OK) {
                throw new IOException("HTTP error: " + responseCode);
            }
            
            // Read response
            StringBuilder response = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(connection.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    response.append(line);
                }
            }
            
            // Parse JSON
            JSONObject manifest = new JSONObject(response.toString());
            JSONArray statesArray = manifest.getJSONArray("states");
            
            List<StateInfo> states = new ArrayList<>();
            for (int i = 0; i < statesArray.length(); i++) {
                JSONObject stateJson = statesArray.getJSONObject(i);
                StateInfo state = new StateInfo();
                state.id = stateJson.getString("id");
                state.name = stateJson.getString("name");
                state.abbrev = stateJson.optString("abbrev", "");
                state.size = stateJson.optLong("size", 0);
                state.placeCount = stateJson.optInt("place_count", 0);
                state.filename = stateJson.optString("filename", state.id + ".db");
                states.add(state);
            }
            
            Log.i(TAG, "Loaded manifest with " + states.size() + " states");
            return states;
            
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }
    
    /**
     * Check if a download is currently in progress.
     */
    public boolean isDownloading() {
        return isDownloading.get();
    }
    
    /**
     * Cancel the current download.
     */
    public void cancelDownload() {
        cancelRequested.set(true);
    }
    
    /**
     * Download a state's database.
     */
    public void downloadState(StateInfo state, DownloadCallback callback) {
        downloadState(state.id, state.filename, callback);
    }
    
    /**
     * Download a state's database by ID.
     */
    public void downloadState(String stateId, String filename, DownloadCallback callback) {
        if (isDownloading.get()) {
            mainHandler.post(() -> callback.onError("A download is already in progress"));
            return;
        }
        
        isDownloading.set(true);
        cancelRequested.set(false);
        
        executor.execute(() -> {
            HttpURLConnection connection = null;
            InputStream input = null;
            FileOutputStream output = null;
            File tempFile = null;
            
            try {
                String downloadUrl = BASE_URL + filename;
                Log.i(TAG, "Downloading: " + downloadUrl);
                
                URL url = new URL(downloadUrl);
                connection = (HttpURLConnection) url.openConnection();
                connection.setRequestMethod("GET");
                connection.setRequestProperty("User-Agent", USER_AGENT);
                connection.setConnectTimeout(CONNECT_TIMEOUT);
                connection.setReadTimeout(READ_TIMEOUT);
                
                // Handle redirects (GitHub releases redirect)
                connection.setInstanceFollowRedirects(true);
                
                int responseCode = connection.getResponseCode();
                if (responseCode != HttpURLConnection.HTTP_OK) {
                    throw new IOException("HTTP error: " + responseCode);
                }
                
                long totalBytes = connection.getContentLengthLong();
                Log.d(TAG, "Download size: " + totalBytes + " bytes");
                
                // Create temp file
                tempFile = new File(database.getDatabaseDir(), stateId + ".db.tmp");
                
                input = new BufferedInputStream(connection.getInputStream());
                output = new FileOutputStream(tempFile);
                
                byte[] buffer = new byte[BUFFER_SIZE];
                long downloadedBytes = 0;
                int bytesRead;
                int lastProgress = -1;
                
                while ((bytesRead = input.read(buffer)) != -1) {
                    // Check for cancellation
                    if (cancelRequested.get()) {
                        Log.i(TAG, "Download cancelled");
                        mainHandler.post(callback::onCancelled);
                        return;
                    }
                    
                    output.write(buffer, 0, bytesRead);
                    downloadedBytes += bytesRead;
                    
                    // Report progress
                    if (totalBytes > 0) {
                        int progress = (int) ((downloadedBytes * 100) / totalBytes);
                        if (progress != lastProgress) {
                            lastProgress = progress;
                            final long finalDownloaded = downloadedBytes;
                            mainHandler.post(() -> 
                                callback.onProgress(progress, finalDownloaded, totalBytes));
                        }
                    }
                }
                
                output.flush();
                output.close();
                output = null;
                
                // Rename temp file to final
                File finalFile = database.getDatabaseFile(stateId);
                if (finalFile.exists()) {
                    finalFile.delete();
                }
                
                if (!tempFile.renameTo(finalFile)) {
                    throw new IOException("Failed to rename temp file");
                }
                
                Log.i(TAG, "Download complete: " + finalFile.getPath());
                mainHandler.post(() -> callback.onComplete(finalFile));
                
            } catch (Exception e) {
                Log.e(TAG, "Download failed: " + e.getMessage(), e);
                mainHandler.post(() -> callback.onError(e.getMessage()));
            } finally {
                isDownloading.set(false);
                
                try {
                    if (input != null) input.close();
                    if (output != null) output.close();
                    if (connection != null) connection.disconnect();
                } catch (Exception ignored) {}
                
                // Clean up temp file on failure
                if (tempFile != null && tempFile.exists()) {
                    tempFile.delete();
                }
            }
        });
    }
    
    /**
     * Delete a downloaded state.
     */
    public boolean deleteState(String stateId) {
        return database.deleteState(stateId);
    }
    
    /**
     * Get total size of all downloaded databases.
     */
    public long getTotalDownloadedSize() {
        long total = 0;
        File dir = database.getDatabaseDir();
        File[] files = dir.listFiles((d, name) -> name.endsWith(".db"));
        if (files != null) {
            for (File file : files) {
                total += file.length();
            }
        }
        return total;
    }
    
    /**
     * Get formatted total size.
     */
    public String getTotalDownloadedSizeFormatted() {
        long bytes = getTotalDownloadedSize();
        if (bytes < 1024) {
            return bytes + " B";
        } else if (bytes < 1024 * 1024) {
            return String.format("%.1f KB", bytes / 1024.0);
        } else {
            return String.format("%.1f MB", bytes / (1024.0 * 1024.0));
        }
    }
    
    /**
     * Shutdown the executor.
     */
    public void shutdown() {
        cancelRequested.set(true);
        executor.shutdown();
    }
}

