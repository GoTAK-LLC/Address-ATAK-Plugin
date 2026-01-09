package com.gotak.address.search;

import android.app.AlertDialog;
import android.content.Context;
import android.content.Intent;
import android.view.LayoutInflater;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.ImageButton;
import android.widget.LinearLayout;
import android.widget.ProgressBar;
import android.widget.TextView;
import android.widget.Toast;

import androidx.recyclerview.widget.LinearLayoutManager;
import androidx.recyclerview.widget.RecyclerView;

import com.atakmap.android.dropdown.DropDown;
import com.atakmap.android.dropdown.DropDownReceiver;
import com.atakmap.android.maps.MapView;
import com.atakmap.coremap.log.Log;
import com.gotak.address.plugin.R;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.List;

/**
 * DropDown for managing offline address data downloads.
 */
public class OfflineDataDropDown extends DropDownReceiver implements
        DropDown.OnStateListener,
        OfflineStateAdapter.StateActionListener {

    public static final String TAG = "OfflineDataDropDown";
    public static final String SHOW_OFFLINE_DATA = "com.gotak.address.SHOW_OFFLINE_DATA";
    public static final String HIDE_OFFLINE_DATA = "com.gotak.address.HIDE_OFFLINE_DATA";

    private final Context pluginContext;
    private final OfflineAddressDatabase database;
    private final OfflineDataManager dataManager;
    
    // UI elements
    private View rootView;
    private RecyclerView statesList;
    private OfflineStateAdapter adapter;
    private TextView storageInfo;
    private LinearLayout loadingContainer;
    private TextView loadingText;
    private TextView errorText;
    private LinearLayout downloadProgressContainer;
    private TextView downloadStateName;
    private ProgressBar downloadProgress;
    private TextView downloadProgressText;
    private Button cancelDownloadButton;
    private Button importFileButton;

    public OfflineDataDropDown(MapView mapView, Context pluginContext) {
        super(mapView);
        this.pluginContext = pluginContext;
        this.database = new OfflineAddressDatabase(pluginContext);
        this.dataManager = new OfflineDataManager(pluginContext, database);
    }

    @Override
    public void onReceive(Context context, Intent intent) {
        String action = intent.getAction();
        if (action == null) return;

        Log.d(TAG, "onReceive: " + action);

        switch (action) {
            case SHOW_OFFLINE_DATA:
                showPanel();
                break;
            case HIDE_OFFLINE_DATA:
                closeDropDown();
                break;
        }
    }

    private void showPanel() {
        try {
            // Inflate layout
            LayoutInflater inflater = LayoutInflater.from(pluginContext);
            rootView = inflater.inflate(R.layout.offline_data_manager, null);

            // Find views
            statesList = rootView.findViewById(R.id.states_list);
            storageInfo = rootView.findViewById(R.id.storage_info);
            loadingContainer = rootView.findViewById(R.id.loading_container);
            loadingText = rootView.findViewById(R.id.loading_text);
            errorText = rootView.findViewById(R.id.error_text);
            downloadProgressContainer = rootView.findViewById(R.id.download_progress_container);
            downloadStateName = rootView.findViewById(R.id.download_state_name);
            downloadProgress = rootView.findViewById(R.id.download_progress);
            downloadProgressText = rootView.findViewById(R.id.download_progress_text);
            cancelDownloadButton = rootView.findViewById(R.id.cancel_download_button);

            // Setup RecyclerView
            adapter = new OfflineStateAdapter(pluginContext, this);
            statesList.setLayoutManager(new LinearLayoutManager(pluginContext));
            statesList.setAdapter(adapter);

            // Setup buttons
            ImageButton closeButton = rootView.findViewById(R.id.close_button);
            closeButton.setOnClickListener(v -> closeDropDown());

            ImageButton refreshButton = rootView.findViewById(R.id.refresh_button);
            refreshButton.setOnClickListener(v -> loadAvailableStates());

            cancelDownloadButton.setOnClickListener(v -> {
                dataManager.cancelDownload();
            });
            
            // Setup import file button
            importFileButton = rootView.findViewById(R.id.import_file_button);
            importFileButton.setOnClickListener(v -> showFileImportDialog());

            // Update storage info
            updateStorageInfo();

            // Show dropdown
            showDropDown(
                    rootView,
                    HALF_WIDTH,
                    FULL_HEIGHT,
                    FULL_WIDTH,
                    HALF_HEIGHT,
                    false,
                    this
            );

            // Load available states
            loadAvailableStates();

        } catch (Exception e) {
            Log.e(TAG, "Error showing panel: " + e.getMessage(), e);
        }
    }

    private void loadAvailableStates() {
        showLoading("Loading available regions...");
        errorText.setVisibility(View.GONE);

        dataManager.fetchAvailableStates(new OfflineDataManager.ManifestCallback() {
            @Override
            public void onSuccess(List<OfflineDataManager.StateInfo> states) {
                hideLoading();
                
                // Merge in any locally downloaded files not in the manifest
                mergeLocalDownloads(states);
                
                adapter.setStates(states);
                
                if (states.isEmpty()) {
                    showError("No regions available. Check your internet connection.");
                }
            }

            @Override
            public void onError(String error) {
                hideLoading();
                showError("Failed to load regions: " + error + 
                         "\n\nMake sure the data repository is configured.");
                
                // Still show downloaded states
                showDownloadedStatesOnly();
            }
        });
    }
    
    private void mergeLocalDownloads(List<OfflineDataManager.StateInfo> states) {
        // Get list of locally downloaded state IDs
        List<String> downloaded = database.getDownloadedStates();
        
        // Check each local download and add if not in manifest
        for (String stateId : downloaded) {
            boolean found = false;
            for (OfflineDataManager.StateInfo state : states) {
                if (state.id.equals(stateId)) {
                    found = true;
                    break;
                }
            }
            
            if (!found) {
                // Local file not in manifest - add it
                OfflineDataManager.StateInfo info = new OfflineDataManager.StateInfo();
                info.id = stateId;
                info.name = formatStateName(stateId);
                info.abbrev = "";
                info.downloaded = true;
                
                OfflineAddressDatabase.DatabaseStats stats = database.getStats(stateId);
                if (stats != null) {
                    info.size = stats.fileSizeBytes;
                    info.placeCount = stats.placeCount;
                }
                
                states.add(info);
            }
        }
    }

    private void showDownloadedStatesOnly() {
        // Show locally downloaded states even if manifest fails
        List<String> downloaded = database.getDownloadedStates();
        if (!downloaded.isEmpty()) {
            // Create StateInfo objects for downloaded states
            java.util.ArrayList<OfflineDataManager.StateInfo> localStates = new java.util.ArrayList<>();
            for (String stateId : downloaded) {
                OfflineDataManager.StateInfo info = new OfflineDataManager.StateInfo();
                info.id = stateId;
                info.name = formatStateName(stateId);
                info.abbrev = "";
                info.downloaded = true;
                
                OfflineAddressDatabase.DatabaseStats stats = database.getStats(stateId);
                if (stats != null) {
                    info.size = stats.fileSizeBytes;
                    info.placeCount = stats.placeCount;
                }
                
                localStates.add(info);
            }
            adapter.setStates(localStates);
        }
    }

    private String formatStateName(String stateId) {
        // Convert "new-york" to "New York"
        String[] parts = stateId.split("-");
        StringBuilder sb = new StringBuilder();
        for (String part : parts) {
            if (sb.length() > 0) sb.append(" ");
            sb.append(part.substring(0, 1).toUpperCase()).append(part.substring(1));
        }
        return sb.toString();
    }

    private void showLoading(String message) {
        loadingContainer.setVisibility(View.VISIBLE);
        loadingText.setText(message);
        statesList.setVisibility(View.GONE);
    }

    private void hideLoading() {
        loadingContainer.setVisibility(View.GONE);
        statesList.setVisibility(View.VISIBLE);
    }

    private void showError(String message) {
        errorText.setText(message);
        errorText.setVisibility(View.VISIBLE);
    }

    private void updateStorageInfo() {
        String size = dataManager.getTotalDownloadedSizeFormatted();
        int count = database.getDownloadedStates().size();
        storageInfo.setText("Downloaded: " + size + " (" + count + " states)");
    }

    @Override
    public void onDownload(OfflineDataManager.StateInfo state) {
        if (dataManager.isDownloading()) {
            Toast.makeText(pluginContext, "A download is already in progress", 
                    Toast.LENGTH_SHORT).show();
            return;
        }

        // Show download progress
        downloadProgressContainer.setVisibility(View.VISIBLE);
        downloadStateName.setText("Downloading " + state.name + "...");
        downloadProgress.setProgress(0);
        downloadProgressText.setText("0%");

        dataManager.downloadState(state, new OfflineDataManager.DownloadCallback() {
            @Override
            public void onProgress(int percent, long downloadedBytes, long totalBytes) {
                downloadProgress.setProgress(percent);
                String progressStr = percent + "%";
                if (totalBytes > 0) {
                    progressStr += String.format(" (%.1f / %.1f MB)",
                            downloadedBytes / (1024.0 * 1024.0),
                            totalBytes / (1024.0 * 1024.0));
                }
                downloadProgressText.setText(progressStr);
            }

            @Override
            public void onComplete(File dbFile) {
                downloadProgressContainer.setVisibility(View.GONE);
                Toast.makeText(pluginContext, state.name + " downloaded successfully!", 
                        Toast.LENGTH_SHORT).show();
                
                // Update UI
                state.downloaded = true;
                adapter.updateState(state);
                updateStorageInfo();
            }

            @Override
            public void onError(String error) {
                downloadProgressContainer.setVisibility(View.GONE);
                Toast.makeText(pluginContext, "Download failed: " + error, 
                        Toast.LENGTH_LONG).show();
            }

            @Override
            public void onCancelled() {
                downloadProgressContainer.setVisibility(View.GONE);
                Toast.makeText(pluginContext, "Download cancelled", 
                        Toast.LENGTH_SHORT).show();
            }
        });
    }

    @Override
    public void onDelete(OfflineDataManager.StateInfo state) {
        // Confirm deletion
        new AlertDialog.Builder(getMapView().getContext())
                .setTitle("Delete " + state.name + "?")
                .setMessage("This will remove the offline address data for " + state.name + 
                           " (" + state.getSizeFormatted() + ").")
                .setPositiveButton("Delete", (dialog, which) -> {
                    if (dataManager.deleteState(state.id)) {
                        Toast.makeText(pluginContext, state.name + " deleted", 
                                Toast.LENGTH_SHORT).show();
                        state.downloaded = false;
                        adapter.updateState(state);
                        updateStorageInfo();
                    } else {
                        Toast.makeText(pluginContext, "Failed to delete " + state.name, 
                                Toast.LENGTH_SHORT).show();
                    }
                })
                .setNegativeButton("Cancel", null)
                .show();
    }

    /**
     * Show file browser dialog to import a database file.
     */
    private void showFileImportDialog() {
        // Scan common directories and show found files
        showManualPathDialog();
    }
    
    /**
     * Fallback dialog for browsing common directories.
     */
    private void showManualPathDialog() {
        // ATAK tools directory is the primary location
        String atakAddressDir = "/sdcard/atak/tools/address";
        String extStorage = android.os.Environment.getExternalStorageDirectory().getAbsolutePath();
        Log.d(TAG, "ATAK address dir: " + atakAddressDir);
        Log.d(TAG, "External storage path: " + extStorage);
        
        // Common directories to check for .db files (ATAK dir first)
        String[] commonPaths = {
                atakAddressDir,
                "/sdcard/atak/tools/address",
                extStorage + "/atak/tools/address",
                extStorage + "/Download",
                extStorage + "/Downloads", 
                "/sdcard/Download",
                "/sdcard/Downloads",
                extStorage + "/atak",
                "/sdcard/atak",
                extStorage
        };
        
        // Find all .db files in common locations
        java.util.ArrayList<File> foundFiles = new java.util.ArrayList<>();
        for (String path : commonPaths) {
            File dir = new File(path);
            if (dir.exists() && dir.isDirectory()) {
                File[] dbFiles = dir.listFiles((d, name) -> name.endsWith(".db"));
                if (dbFiles != null) {
                    for (File f : dbFiles) {
                        foundFiles.add(f);
                    }
                }
            }
        }
        
        if (foundFiles.isEmpty()) {
            // No files found - show manual path entry with helpful info
            Log.w(TAG, "No .db files found in common paths");
            showManualPathEntry();
            return;
        }
        
        Log.i(TAG, "Found " + foundFiles.size() + " .db files");
        
        // Show list of found files
        String[] fileNames = new String[foundFiles.size()];
        for (int i = 0; i < foundFiles.size(); i++) {
            File f = foundFiles.get(i);
            long sizeMB = f.length() / (1024 * 1024);
            fileNames[i] = f.getName() + " (" + sizeMB + " MB)\n" + f.getParent();
        }
        
        new AlertDialog.Builder(getMapView().getContext())
                .setTitle("Select Database File")
                .setItems(fileNames, (dialog, which) -> {
                    importDatabaseFile(foundFiles.get(which));
                })
                .setNeutralButton("Enter Path", (dialog, which) -> {
                    showManualPathEntry();
                })
                .setNegativeButton("Cancel", null)
                .show();
    }
    
    /**
     * Show manual path entry dialog.
     */
    private void showManualPathEntry() {
        String atakAddressDir = "/sdcard/atak/tools/address";
        
        final EditText input = new EditText(getMapView().getContext());
        input.setHint(atakAddressDir + "/virginia.db");
        input.setText(atakAddressDir + "/");
        input.setSelectAllOnFocus(true);
        
        new AlertDialog.Builder(getMapView().getContext())
                .setTitle("Import Database File")
                .setMessage("No .db files found.\n\n" +
                           "Copy .db files to:\n" + atakAddressDir + "\n\n" +
                           "Or enter the full path:")
                .setView(input)
                .setPositiveButton("Import", (dialog, which) -> {
                    String path = input.getText().toString().trim();
                    if (!path.isEmpty()) {
                        File file = new File(path);
                        Log.d(TAG, "Trying to import: " + path + " exists=" + file.exists());
                        if (file.exists() && file.getName().endsWith(".db")) {
                            importDatabaseFile(file);
                        } else {
                            Toast.makeText(pluginContext, 
                                    "File not found: " + path + "\nExists: " + file.exists(), 
                                    Toast.LENGTH_LONG).show();
                        }
                    }
                })
                .setNegativeButton("Cancel", null)
                .show();
    }
    
    /**
     * Import a database file by copying it to the plugin's data directory.
     */
    private void importDatabaseFile(File sourceFile) {
        if (!sourceFile.exists()) {
            Toast.makeText(pluginContext, "File not found: " + sourceFile.getPath(), 
                    Toast.LENGTH_LONG).show();
            return;
        }
        
        if (!sourceFile.getName().endsWith(".db")) {
            Toast.makeText(pluginContext, "Invalid file type. Please select a .db file.", 
                    Toast.LENGTH_LONG).show();
            return;
        }
        
        // Extract state ID from filename (e.g., "virginia.db" -> "virginia")
        String filename = sourceFile.getName();
        String stateId = filename.substring(0, filename.length() - 3); // Remove .db
        
        // Confirm import
        long sizeMB = sourceFile.length() / (1024 * 1024);
        new AlertDialog.Builder(getMapView().getContext())
                .setTitle("Import " + formatStateName(stateId) + "?")
                .setMessage("File: " + filename + "\n" +
                           "Size: " + sizeMB + " MB\n\n" +
                           "This will copy the database to the plugin's storage.")
                .setPositiveButton("Import", (dialog, which) -> {
                    performFileImport(sourceFile, stateId);
                })
                .setNegativeButton("Cancel", null)
                .show();
    }
    
    /**
     * Perform the actual file copy operation.
     */
    private void performFileImport(File sourceFile, String stateId) {
        // Verify source file exists and is readable
        if (!sourceFile.exists()) {
            Toast.makeText(pluginContext, 
                    "Source file does not exist: " + sourceFile.getAbsolutePath(), 
                    Toast.LENGTH_LONG).show();
            return;
        }
        
        if (!sourceFile.canRead()) {
            Toast.makeText(pluginContext, 
                    "Cannot read source file (permission denied): " + sourceFile.getAbsolutePath(), 
                    Toast.LENGTH_LONG).show();
            return;
        }
        
        // Verify destination directory exists
        File destDir = database.getDatabaseDir();
        Log.d(TAG, "Destination directory: " + destDir.getAbsolutePath());
        if (!destDir.exists()) {
            boolean created = destDir.mkdirs();
            Log.d(TAG, "Created destination dir: " + created);
            if (!created) {
                Toast.makeText(pluginContext, 
                        "Cannot create destination directory: " + destDir.getAbsolutePath(), 
                        Toast.LENGTH_LONG).show();
                return;
            }
        }
        
        // Show progress
        downloadProgressContainer.setVisibility(View.VISIBLE);
        downloadStateName.setText("Importing " + formatStateName(stateId) + "...");
        downloadProgress.setIndeterminate(true);
        downloadProgressText.setText("Copying " + (sourceFile.length() / 1024 / 1024) + " MB...");
        
        // Run copy in background
        new Thread(() -> {
            try {
                File destFile = database.getDatabaseFile(stateId);
                Log.i(TAG, "Copying from: " + sourceFile.getAbsolutePath());
                Log.i(TAG, "Copying to: " + destFile.getAbsolutePath());
                
                copyFile(sourceFile, destFile);
                
                Log.i(TAG, "Copy complete. Dest size: " + destFile.length());
                
                // Update UI on main thread
                getMapView().post(() -> {
                    downloadProgressContainer.setVisibility(View.GONE);
                    Toast.makeText(pluginContext, 
                            formatStateName(stateId) + " imported successfully!", 
                            Toast.LENGTH_SHORT).show();
                    updateStorageInfo();
                    loadAvailableStates(); // Refresh the list
                });
                
            } catch (Exception e) {
                Log.e(TAG, "Import failed: " + e.getMessage(), e);
                e.printStackTrace();
                final String errorMsg = e.getMessage();
                getMapView().post(() -> {
                    downloadProgressContainer.setVisibility(View.GONE);
                    Toast.makeText(pluginContext, 
                            "Import failed: " + errorMsg, 
                            Toast.LENGTH_LONG).show();
                });
            }
        }).start();
    }
    
    /**
     * Copy a file using NIO for efficiency.
     */
    private void copyFile(File source, File dest) throws IOException {
        if (dest.exists()) {
            dest.delete();
        }
        
        try (FileInputStream fis = new FileInputStream(source);
             FileOutputStream fos = new FileOutputStream(dest);
             FileChannel sourceChannel = fis.getChannel();
             FileChannel destChannel = fos.getChannel()) {
            
            destChannel.transferFrom(sourceChannel, 0, sourceChannel.size());
        }
        
        Log.i(TAG, "Copied " + source.getPath() + " to " + dest.getPath());
    }

    @Override
    public void onDropDownSelectionRemoved() {}

    @Override
    public void onDropDownClose() {}

    @Override
    public void onDropDownSizeChanged(double width, double height) {}

    @Override
    public void onDropDownVisible(boolean visible) {}

    @Override
    protected void disposeImpl() {
        dataManager.shutdown();
        database.close();
    }
}

