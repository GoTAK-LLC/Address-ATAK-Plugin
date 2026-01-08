package com.gotak.address.search;

import android.app.AlertDialog;
import android.content.Context;
import android.content.Intent;
import android.content.res.Configuration;
import android.graphics.Color;
import android.os.Handler;
import android.os.Looper;
import android.text.Editable;
import android.text.TextWatcher;
import android.view.Gravity;
import android.view.KeyEvent;
import android.view.LayoutInflater;
import android.view.View;
import android.view.inputmethod.EditorInfo;
import android.view.inputmethod.InputMethodManager;
import android.widget.Button;
import android.widget.EditText;
import android.widget.ImageButton;
import android.widget.LinearLayout;
import android.widget.TextView;

import androidx.recyclerview.widget.LinearLayoutManager;
import androidx.recyclerview.widget.RecyclerView;

import com.atakmap.android.bloodhound.BloodHoundTool;
import com.atakmap.android.dropdown.DropDown;
import com.atakmap.android.dropdown.DropDownReceiver;
import com.atakmap.android.ipc.AtakBroadcast;
import com.atakmap.android.maps.MapGroup;
import com.atakmap.android.maps.Marker;
import com.gotak.address.plugin.R;
import com.atakmap.android.maps.MapView;
import com.atakmap.coremap.log.Log;
import com.atakmap.coremap.maps.coords.GeoPoint;

import android.graphics.Bitmap;
import android.graphics.Canvas;
import android.graphics.Paint;
import android.graphics.Path;

import java.util.List;
import java.util.UUID;

/**
 * DropDown receiver for the address search panel.
 * Provides location search using Nominatim (OpenStreetMap) API.
 */
public class AddressSearchDropDown extends DropDownReceiver implements
        DropDown.OnStateListener,
        SearchResultsAdapter.OnResultClickListener,
        HistoryAdapter.HistoryItemListener {

    public static final String TAG = "AddressSearchDropDown";
    public static final String SHOW_SEARCH = "com.gotak.address.SHOW_ADDRESS_SEARCH";
    public static final String HIDE_SEARCH = "com.gotak.address.HIDE_ADDRESS_SEARCH";

    private static final long DEBOUNCE_DELAY_MS = 300;
    private static final int MIN_QUERY_LENGTH = 2;

    private final Context pluginContext;
    private final NominatimApiClient apiClient;
    private final Handler mainHandler;
    private final SearchHistoryManager historyManager;

    // UI elements
    private View rootView;
    private EditText searchInput;
    private ImageButton clearButton;
    private ImageButton closeButton;
    private TextView searchStatus;
    private TextView sectionHeader;
    private RecyclerView resultsRecyclerView;
    private SearchResultsAdapter resultsAdapter;

    // History UI elements
    private LinearLayout historyContainer;
    private RecyclerView historyRecyclerView;
    private HistoryAdapter historyAdapter;
    private TextView clearHistoryButton;
    
    // Offline data button
    private Button offlineDataButton;

    // Debounce handling
    private final Runnable searchRunnable;
    private String pendingQuery = "";

    public AddressSearchDropDown(MapView mapView, Context pluginContext) {
        super(mapView);
        this.pluginContext = pluginContext;
        this.apiClient = new NominatimApiClient(pluginContext); // Use context for offline DB
        this.mainHandler = new Handler(Looper.getMainLooper());
        this.historyManager = new SearchHistoryManager(pluginContext);

        // Create debounced search runnable
        this.searchRunnable = () -> {
            if (pendingQuery.length() >= MIN_QUERY_LENGTH) {
                performSearch(pendingQuery);
            }
        };
    }

    @Override
    public void onReceive(Context context, Intent intent) {
        String action = intent.getAction();
        if (action == null) return;

        Log.d(TAG, "onReceive: " + action);

        switch (action) {
            case SHOW_SEARCH:
                showSearchPanel();
                break;
            case HIDE_SEARCH:
                closeDropDown();
                break;
        }
    }

    private void showSearchPanel() {
        try {
            // Inflate layout
            LayoutInflater inflater = LayoutInflater.from(pluginContext);
            rootView = inflater.inflate(R.layout.address_search_panel, null);

            // Find views
            searchInput = rootView.findViewById(R.id.search_input);
            clearButton = rootView.findViewById(R.id.clear_button);
            closeButton = rootView.findViewById(R.id.close_button);
            searchStatus = rootView.findViewById(R.id.search_status);
            sectionHeader = rootView.findViewById(R.id.section_header);
            resultsRecyclerView = rootView.findViewById(R.id.search_results);

            // History views
            historyContainer = rootView.findViewById(R.id.history_container);
            historyRecyclerView = rootView.findViewById(R.id.history_results);
            clearHistoryButton = rootView.findViewById(R.id.clear_history_button);

            // Setup search results RecyclerView
            resultsAdapter = new SearchResultsAdapter(pluginContext, this);
            resultsRecyclerView.setLayoutManager(new LinearLayoutManager(pluginContext));
            resultsRecyclerView.setAdapter(resultsAdapter);

            // Setup history RecyclerView
            historyAdapter = new HistoryAdapter(pluginContext, this);
            historyRecyclerView.setLayoutManager(new LinearLayoutManager(pluginContext));
            historyRecyclerView.setAdapter(historyAdapter);
            
            // Setup offline data button
            offlineDataButton = rootView.findViewById(R.id.offline_data_button);
            offlineDataButton.setOnClickListener(v -> {
                // Close this dropdown and open offline data manager
                closeDropDown();
                Intent offlineIntent = new Intent(OfflineDataDropDown.SHOW_OFFLINE_DATA);
                AtakBroadcast.getInstance().sendBroadcast(offlineIntent);
            });

            // Setup search input
            setupSearchInput();

            // Setup buttons
            setupButtons();

            // Show history if available
            refreshHistoryView();

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

            // Auto-focus keyboard behavior:
            // - Portrait mode (any device): auto-focus and show keyboard
            // - Tablet in landscape: auto-focus and show keyboard
            // - Phone in landscape: do NOT auto-focus (keyboard takes too much space)
            Configuration config = pluginContext.getResources().getConfiguration();
            boolean isPortrait = config.orientation == Configuration.ORIENTATION_PORTRAIT;
            boolean isLandscape = config.orientation == Configuration.ORIENTATION_LANDSCAPE;
            int screenSize = config.screenLayout & Configuration.SCREENLAYOUT_SIZE_MASK;
            boolean isTablet = screenSize >= Configuration.SCREENLAYOUT_SIZE_LARGE;
            boolean isPhone = !isTablet;

            // Only auto-focus if: portrait mode, OR tablet in landscape
            // Do NOT auto-focus: phone in landscape (let user tap to focus)
            boolean shouldAutoFocus = isPortrait || (isLandscape && isTablet);

            if (shouldAutoFocus) {
                searchInput.requestFocus();
                searchInput.postDelayed(this::showKeyboard, 200);
            }
        } catch (Exception e) {
            Log.e(TAG, "Error showing search panel: " + e.getMessage(), e);
        }
    }

    private void setupSearchInput() {
        // Text change listener with debouncing
        searchInput.addTextChangedListener(new TextWatcher() {
            @Override
            public void beforeTextChanged(CharSequence s, int start, int count, int after) {}

            @Override
            public void onTextChanged(CharSequence s, int start, int before, int count) {}

            @Override
            public void afterTextChanged(Editable s) {
                String text = s != null ? s.toString().trim() : "";
                pendingQuery = text;

                // Show/hide clear button
                clearButton.setVisibility(text.isEmpty() ? View.GONE : View.VISIBLE);

                // Cancel any pending search
                mainHandler.removeCallbacks(searchRunnable);

                if (text.length() >= MIN_QUERY_LENGTH) {
                    // Hide history when searching
                    historyContainer.setVisibility(View.GONE);
                    // Debounce: wait before searching
                    mainHandler.postDelayed(searchRunnable, DEBOUNCE_DELAY_MS);
                } else {
                    // Show history when not searching
                    showIdle();
                    refreshHistoryView();
                }
            }
        });

        // Handle Enter key for immediate search
        searchInput.setOnEditorActionListener((v, actionId, event) -> {
            if (actionId == EditorInfo.IME_ACTION_SEARCH ||
                    (event != null && event.getKeyCode() == KeyEvent.KEYCODE_ENTER
                            && event.getAction() == KeyEvent.ACTION_DOWN)) {
                mainHandler.removeCallbacks(searchRunnable);
                String query = searchInput.getText().toString().trim();
                if (query.length() >= MIN_QUERY_LENGTH) {
                    performSearch(query);
                }
                hideKeyboard();
                return true;
            }
            return false;
        });
    }

    private void setupButtons() {
        // Clear button
        clearButton.setOnClickListener(v -> {
            searchInput.setText("");
            resultsAdapter.clear();
            showIdle();
            refreshHistoryView();
        });

        // Close button
        closeButton.setOnClickListener(v -> {
            resultsAdapter.clear();
            closeDropDown();
        });

        // Clear history button
        clearHistoryButton.setOnClickListener(v -> {
            historyManager.clearHistory();
            refreshHistoryView();
        });
    }

    private void refreshHistoryView() {
        List<NominatimSearchResult> history = historyManager.getHistory();
        if (history.isEmpty()) {
            historyContainer.setVisibility(View.GONE);
        } else {
            historyAdapter.setItems(history);
            historyContainer.setVisibility(View.VISIBLE);
        }
    }

    private void performSearch(String query) {
        Log.i(TAG, "Searching for: " + query);
        showSearching();

        apiClient.search(query, new NominatimApiClient.SearchCallback() {
            @Override
            public void onSuccess(List<NominatimSearchResult> results) {
                Log.i(TAG, "Got " + results.size() + " results");
                showResults(results);
            }

            @Override
            public void onError(String errorMessage) {
                Log.e(TAG, "Search error: " + errorMessage);
                showError(errorMessage);
            }
        });
    }

    private void showIdle() {
        searchStatus.setVisibility(View.GONE);
        sectionHeader.setVisibility(View.GONE);
        resultsRecyclerView.setVisibility(View.GONE);
    }

    private void showSearching() {
        historyContainer.setVisibility(View.GONE);
        searchStatus.setText(R.string.searching);
        searchStatus.setVisibility(View.VISIBLE);
        sectionHeader.setVisibility(View.GONE);
        resultsRecyclerView.setVisibility(View.GONE);
    }

    private void showResults(List<NominatimSearchResult> results) {
        historyContainer.setVisibility(View.GONE);
        if (results.isEmpty()) {
            searchStatus.setText(R.string.no_results);
            searchStatus.setVisibility(View.VISIBLE);
            sectionHeader.setVisibility(View.GONE);
            resultsRecyclerView.setVisibility(View.GONE);
        } else {
            searchStatus.setVisibility(View.GONE);
            sectionHeader.setVisibility(View.VISIBLE);
            resultsRecyclerView.setVisibility(View.VISIBLE);
            resultsAdapter.setResults(results);
        }
    }

    private void showError(String message) {
        String errorText = pluginContext.getString(R.string.search_error) + ": " + message;
        searchStatus.setText(errorText);
        searchStatus.setVisibility(View.VISIBLE);
        sectionHeader.setVisibility(View.GONE);
        resultsRecyclerView.setVisibility(View.GONE);
    }

    @Override
    public void onResultClick(NominatimSearchResult result) {
        navigateToResult(result);
        // Add to history when selected from search results
        historyManager.addToHistory(result);
        // Keep pane open - clear search and show updated history
        searchInput.setText("");
        resultsAdapter.clear();
        showIdle();
        refreshHistoryView();
    }

    @Override
    public void onHistoryItemClick(NominatimSearchResult result) {
        navigateToResult(result);
        // Move to top of history when clicked
        historyManager.addToHistory(result);
        // Keep pane open - refresh history to show updated order
        refreshHistoryView();
    }

    @Override
    public void onHistoryItemRemove(NominatimSearchResult result) {
        historyManager.removeFromHistory(result.getPlaceId());
        refreshHistoryView();
    }

    @Override
    public void onHistoryItemNavigate(NominatimSearchResult result) {
        startBloodhoundNavigation(result);
    }

    @Override
    public void onHistoryItemDropMarker(NominatimSearchResult result) {
        dropMarkerAtLocation(result);
    }

    @Override
    public void onResultDropMarker(NominatimSearchResult result) {
        dropMarkerAtLocation(result);
        // Add to history when a marker is dropped
        historyManager.addToHistory(result);
    }

    @Override
    public void onResultNavigate(NominatimSearchResult result) {
        startBloodhoundNavigation(result);
        // Add to history when navigating
        historyManager.addToHistory(result);
    }

    /**
     * Start Bloodhound navigation to the given location.
     * This drops a marker at the location and activates the Bloodhound tool.
     */
    private void startBloodhoundNavigation(NominatimSearchResult result) {
        Log.i(TAG, "Starting Bloodhound navigation to: " + result.getName());
        
        // Create GeoPoint from result
        GeoPoint point = new GeoPoint(result.getLatitude(), result.getLongitude());
        
        // Generate unique ID for the navigation marker
        String uid = "address-nav-" + UUID.randomUUID().toString().substring(0, 8);
        
        // Create the marker
        Marker marker = new Marker(point, uid);
        marker.setType("b-m-p-w-GOTO");  // GOTO waypoint type (common for navigation targets)
        
        // Set title from location name
        String title = result.getName();
        marker.setTitle(title);
        
        // Set full address as remarks
        marker.setMetaString("remarks", result.getDisplayName());
        
        // Make it persistent
        marker.setMetaBoolean("readiness", true);
        marker.setMetaBoolean("archive", true);
        marker.setMetaString("how", "h-g-i-g-o");
        
        // Add to the map
        MapGroup rootGroup = getMapView().getRootGroup();
        if (rootGroup != null) {
            MapGroup userObjects = rootGroup.findMapGroup("User Objects");
            if (userObjects == null) {
                userObjects = rootGroup.addGroup("User Objects");
            }
            userObjects.addItem(marker);
            Log.d(TAG, "Navigation marker dropped: " + title);
            
            // Navigate to the marker location on map
            navigateToResult(result);
            
            // Start Bloodhound navigation to the marker
            Intent bloodhoundIntent = new Intent();
            bloodhoundIntent.setAction(BloodHoundTool.BLOOD_HOUND);
            bloodhoundIntent.putExtra("uid", uid);
            AtakBroadcast.getInstance().sendBroadcast(bloodhoundIntent);
            Log.d(TAG, "Bloodhound started for: " + uid);
        } else {
            Log.e(TAG, "Could not find root map group");
        }
    }

    /**
     * Marker type options with CoT type codes and MIL-STD-2525 colors/shapes.
     * Shape types: 0=rectangle (friendly), 1=diamond (hostile), 2=square (neutral), 3=circle (unknown)
     */
    private static class MarkerType {
        final String name;
        final String cotType;
        final int color;
        final int shapeType; // 0=rect, 1=diamond, 2=square, 3=circle
        
        MarkerType(String name, String cotType, int color, int shapeType) {
            this.name = name;
            this.cotType = cotType;
            this.color = color;
            this.shapeType = shapeType;
        }
    }
    
    // MIL-STD-2525 standard colors
    private static final int COLOR_FRIENDLY = Color.rgb(128, 224, 255);  // Light blue
    private static final int COLOR_HOSTILE = Color.rgb(255, 128, 128);   // Light red/salmon
    private static final int COLOR_NEUTRAL = Color.rgb(170, 255, 170);   // Light green
    private static final int COLOR_UNKNOWN = Color.rgb(255, 255, 128);   // Light yellow
    
    private static final MarkerType[] MARKER_TYPES = {
        new MarkerType("Friendly", "a-f-G", COLOR_FRIENDLY, 0),
        new MarkerType("Hostile", "a-h-G", COLOR_HOSTILE, 1),
        new MarkerType("Neutral", "a-n-G", COLOR_NEUTRAL, 2),
        new MarkerType("Unknown", "a-u-G", COLOR_UNKNOWN, 3),
    };
    
    /**
     * Create a MIL-STD-2525 style marker icon bitmap.
     */
    private Bitmap createMarkerIcon(int color, int shapeType, int size) {
        Bitmap bitmap = Bitmap.createBitmap(size, size, Bitmap.Config.ARGB_8888);
        Canvas canvas = new Canvas(bitmap);
        
        Paint fillPaint = new Paint(Paint.ANTI_ALIAS_FLAG);
        fillPaint.setColor(color);
        fillPaint.setStyle(Paint.Style.FILL);
        
        Paint strokePaint = new Paint(Paint.ANTI_ALIAS_FLAG);
        strokePaint.setColor(Color.BLACK);
        strokePaint.setStyle(Paint.Style.STROKE);
        strokePaint.setStrokeWidth(3);
        
        int padding = 4;
        int w = size - padding * 2;
        int h = size - padding * 2;
        int cx = size / 2;
        int cy = size / 2;
        
        switch (shapeType) {
            case 0: // Friendly - Rectangle (rounded corners)
                canvas.drawRoundRect(padding, padding + h/6, size - padding, size - padding - h/6, 8, 8, fillPaint);
                canvas.drawRoundRect(padding, padding + h/6, size - padding, size - padding - h/6, 8, 8, strokePaint);
                break;
                
            case 1: // Hostile - Diamond
                Path diamond = new Path();
                diamond.moveTo(cx, padding);
                diamond.lineTo(size - padding, cy);
                diamond.lineTo(cx, size - padding);
                diamond.lineTo(padding, cy);
                diamond.close();
                canvas.drawPath(diamond, fillPaint);
                canvas.drawPath(diamond, strokePaint);
                break;
                
            case 2: // Neutral - Square
                canvas.drawRect(padding, padding, size - padding, size - padding, fillPaint);
                canvas.drawRect(padding, padding, size - padding, size - padding, strokePaint);
                break;
                
                case 3: // Unknown - Quatrefoil (4-leaf clover)
                float radius = w / 4.5f;
                // Draw 4 circles to form quatrefoil
                canvas.drawCircle(cx, cy - radius, radius, fillPaint);  // Top
                canvas.drawCircle(cx, cy + radius, radius, fillPaint);  // Bottom
                canvas.drawCircle(cx - radius, cy, radius, fillPaint);  // Left
                canvas.drawCircle(cx + radius, cy, radius, fillPaint);  // Right
                // Draw strokes
                canvas.drawCircle(cx, cy - radius, radius, strokePaint);
                canvas.drawCircle(cx, cy + radius, radius, strokePaint);
                canvas.drawCircle(cx - radius, cy, radius, strokePaint);
                canvas.drawCircle(cx + radius, cy, radius, strokePaint);
                break;
        }
        
        return bitmap;
    }
    
    /**
     * Show compact dialog to select marker type with MIL-STD-2525 style icons in a row.
     */
    private void dropMarkerAtLocation(NominatimSearchResult result) {
        Log.i(TAG, "Showing marker type dialog for: " + result.getName());
        
        // Build the dialog with marker type options
        AlertDialog.Builder builder = new AlertDialog.Builder(getMapView().getContext());
        builder.setTitle("Marker Type");
        
        // Create a horizontal row of icons
        LinearLayout layout = new LinearLayout(getMapView().getContext());
        layout.setOrientation(LinearLayout.HORIZONTAL);
        layout.setGravity(Gravity.CENTER);
        layout.setPadding(16, 16, 16, 16);
        
        final AlertDialog[] dialogHolder = new AlertDialog[1];
        
        int iconSize = 56;
        
        for (MarkerType markerType : MARKER_TYPES) {
            // Create the MIL-STD-2525 style icon
            Bitmap iconBitmap = createMarkerIcon(markerType.color, markerType.shapeType, iconSize);
            
            // Create ImageView for the marker icon
            android.widget.ImageView iconView = new android.widget.ImageView(getMapView().getContext());
            iconView.setImageBitmap(iconBitmap);
            iconView.setClickable(true);
            iconView.setFocusable(true);
            iconView.setBackgroundResource(android.R.drawable.list_selector_background);
            iconView.setPadding(12, 12, 12, 12);
            
            LinearLayout.LayoutParams iconParams = new LinearLayout.LayoutParams(iconSize + 24, iconSize + 24);
            iconParams.setMargins(8, 0, 8, 0);
            iconView.setLayoutParams(iconParams);
            
            // Handle icon click
            final String cotType = markerType.cotType;
            iconView.setOnClickListener(v -> {
                if (dialogHolder[0] != null) {
                    dialogHolder[0].dismiss();
                }
                createMarkerWithType(result, cotType);
            });
            
            layout.addView(iconView);
        }
        
        builder.setView(layout);
        builder.setNegativeButton("Cancel", null);
        
        AlertDialog dialog = builder.create();
        dialogHolder[0] = dialog;
        dialog.show();
    }
    
    /**
     * Create and place the marker with the specified CoT type.
     */
    private void createMarkerWithType(NominatimSearchResult result, String cotType) {
        Log.i(TAG, "Dropping marker at: " + result.getName() + " with type: " + cotType);
        
        // Create GeoPoint from result
        GeoPoint point = new GeoPoint(result.getLatitude(), result.getLongitude());
        
        // Generate unique ID for this marker
        String uid = "address-marker-" + UUID.randomUUID().toString().substring(0, 8);
        
        // Create the marker
        Marker marker = new Marker(point, uid);
        marker.setType(cotType);
        
        // Set title from location name
        String title = result.getName();
        marker.setTitle(title);
        
        // Set full address as remarks
        marker.setMetaString("remarks", result.getDisplayName());
        
        // Make it persistent and show callsign
        marker.setMetaBoolean("readiness", true);
        marker.setMetaBoolean("archive", true);
        marker.setMetaString("how", "h-g-i-g-o"); // Human, ground, individual, gps, other
        
        // Add to the map
        MapGroup rootGroup = getMapView().getRootGroup();
        if (rootGroup != null) {
            MapGroup userObjects = rootGroup.findMapGroup("User Objects");
            if (userObjects == null) {
                // Create User Objects group if it doesn't exist
                userObjects = rootGroup.addGroup("User Objects");
            }
            userObjects.addItem(marker);
            Log.d(TAG, "Marker dropped: " + title + " (" + cotType + ")");
            
            // Navigate to the marker location
            navigateToResult(result);
        } else {
            Log.e(TAG, "Could not find root map group");
        }
    }

    private void navigateToResult(NominatimSearchResult result) {
        Log.i(TAG, "Navigating to: " + result.getName() + " at " +
                result.getLatitude() + ", " + result.getLongitude());

        // Create GeoPoint and navigate
        GeoPoint point = new GeoPoint(result.getLatitude(), result.getLongitude());
        double zoomLevel = result.getZoomLevel();

        // Pan and zoom to the selected location
        getMapView().getMapController().panTo(point, true);
        getMapView().getMapController().zoomTo(zoomLevel, true);

        // Don't close the panel - let caller handle UI updates
    }

    private void showKeyboard() {
        InputMethodManager imm = (InputMethodManager) pluginContext
                .getSystemService(Context.INPUT_METHOD_SERVICE);
        if (imm != null && searchInput != null) {
            imm.showSoftInput(searchInput, InputMethodManager.SHOW_IMPLICIT);
        }
    }

    private void hideKeyboard() {
        InputMethodManager imm = (InputMethodManager) pluginContext
                .getSystemService(Context.INPUT_METHOD_SERVICE);
        if (imm != null && searchInput != null) {
            imm.hideSoftInputFromWindow(searchInput.getWindowToken(), 0);
        }
    }

    @Override
    public void onDropDownSelectionRemoved() {
        cleanup();
    }

    @Override
    public void onDropDownClose() {
        cleanup();
    }

    @Override
    public void onDropDownSizeChanged(double width, double height) {}

    @Override
    public void onDropDownVisible(boolean visible) {}

    private void cleanup() {
        mainHandler.removeCallbacks(searchRunnable);
        if (resultsAdapter != null) {
            resultsAdapter.clear();
        }
    }

    @Override
    protected void disposeImpl() {
        cleanup();
        apiClient.shutdown();
    }
}
