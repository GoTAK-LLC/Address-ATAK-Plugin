package com.gotak.address.selfgeo;

import android.content.Context;
import android.content.SharedPreferences;
import android.location.Address;
import android.os.Handler;
import android.os.Looper;
import android.preference.PreferenceManager;
import android.view.MotionEvent;

import com.atakmap.android.maps.MapView;
import com.atakmap.android.maps.PointMapItem;
import com.atakmap.android.user.geocode.GeocodeManager;
import com.atakmap.android.widgets.AbstractWidgetMapComponent;
import com.atakmap.android.widgets.LinearLayoutWidget;
import com.atakmap.android.widgets.MapWidget;
import com.atakmap.android.widgets.RootLayoutWidget;
import com.atakmap.android.widgets.TextWidget;
import com.atakmap.coremap.log.Log;
import com.atakmap.coremap.maps.coords.GeoPoint;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Widget that displays the user's current address above their callsign on the map.
 * Uses ATAK's GeocodeManager to convert GPS coordinates to an address.
 */
public class SelfLocationWidget extends AbstractWidgetMapComponent 
        implements MapWidget.OnClickListener, SharedPreferences.OnSharedPreferenceChangeListener {
    
    private static final String TAG = "SelfLocationWidget";
    
    // Preference keys
    public static final String PREF_SHOW_SELF_ADDRESS = "address_show_self_location";
    public static final String PREF_REFRESH_PERIOD = "address_refresh_period";
    
    // Cache keys for persisting address across restarts
    private static final String CACHE_LAST_ADDRESS = "address_cache_last_address";
    private static final String CACHE_LAST_LAT = "address_cache_last_lat";
    private static final String CACHE_LAST_LON = "address_cache_last_lon";
    
    // Default values
    private static final boolean DEFAULT_SHOW_SELF_ADDRESS = true;
    private static final int DEFAULT_REFRESH_PERIOD_SECONDS = 5;
    
    // Widget positioning - matches callsign area style
    private static final int FONT_SIZE = 2;
    private static final float HORIZONTAL_MARGIN = 16f;
    private static final float VERTICAL_MARGIN = 4f;
    
    // Colors (ARGB format)
    private static final int COLOR_CYAN = 0xFF00FFFF;
    
    // Minimum distance (in meters) before re-geocoding
    // 50 feet = 15.24 meters - only geocode when user moves significantly
    private static final double MIN_DISTANCE_FOR_GEOCODE = 15.24;
    
    private MapView mapView;
    private Context pluginContext;
    private SharedPreferences prefs;
    
    private LinearLayoutWidget layout;
    private TextWidget addressWidget;
    
    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> geocodingTask;
    private Handler mainHandler;
    
    // Photon API fallback geocoder
    private ReverseGeocoder photonGeocoder;
    
    private GeoPoint lastGeocodedPoint;
    private String currentAddress = "";
    private String lastDisplayedText = "";
    private int lastDisplayedColor = 0;
    
    @Override
    protected void onCreateWidgets(Context context, android.content.Intent intent, MapView mapView) {
        Log.d(TAG, "onCreateWidgets");
        this.mapView = mapView;
        this.pluginContext = context;
        this.prefs = PreferenceManager.getDefaultSharedPreferences(context);
        this.mainHandler = new Handler(Looper.getMainLooper());
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        this.photonGeocoder = new ReverseGeocoder();
        
        // Register preference listener
        prefs.registerOnSharedPreferenceChangeListener(this);
        
        // Load cached address from previous session
        loadCachedAddress();
        
        // Create widget in bottom right corner, directly above the callsign
        RootLayoutWidget root = (RootLayoutWidget) mapView.getComponentExtra("rootLayoutWidget");
        if (root != null) {
            // Get the BOTTOM_RIGHT layout directly (where callsign lives)
            layout = root.getLayout(RootLayoutWidget.BOTTOM_RIGHT);
            
            addressWidget = new TextWidget("", FONT_SIZE);
            addressWidget.setName("SelfAddressWidget");
            // Right margin to align with callsign, minimal vertical margins
            addressWidget.setMargins(0f, VERTICAL_MARGIN, HORIZONTAL_MARGIN, VERTICAL_MARGIN);
            addressWidget.addOnClickListener(this);
            addressWidget.setVisible(false);
            
            // Add at the end of the layout (just above where callsign appears)
            // Using a high index ensures it's near the bottom of the stack
            int childCount = layout.getChildCount();
            layout.addChildWidgetAt(Math.max(0, childCount), addressWidget);
        }
        
        // Start geocoding if enabled
        if (isGeocodingEnabled()) {
            startGeocoding();
        }
    }
    
    @Override
    protected void onDestroyWidgets(Context context, MapView mapView) {
        Log.d(TAG, "onDestroyWidgets");
        
        stopGeocoding();
        
        if (prefs != null) {
            prefs.unregisterOnSharedPreferenceChangeListener(this);
        }
        
        if (scheduler != null) {
            scheduler.shutdown();
        }
        
        if (photonGeocoder != null) {
            photonGeocoder.shutdown();
        }
        
        if (layout != null && addressWidget != null) {
            layout.removeWidget(addressWidget);
        }
        
        addressWidget = null;
        layout = null;
    }
    
    @Override
    public void onMapWidgetClick(MapWidget widget, MotionEvent event) {
        Log.d(TAG, "Widget clicked - refreshing geocoding");
        // Force refresh on click - reset location cache but keep display stable
        lastGeocodedPoint = null;
        performGeocoding();
    }
    
    @Override
    public void onSharedPreferenceChanged(SharedPreferences sharedPreferences, String key) {
        if (PREF_SHOW_SELF_ADDRESS.equals(key)) {
            if (isGeocodingEnabled()) {
                startGeocoding();
            } else {
                stopGeocoding();
                updateWidgetVisibility(false);
            }
        } else if (PREF_REFRESH_PERIOD.equals(key)) {
            // Restart with new period
            if (isGeocodingEnabled()) {
                stopGeocoding();
                startGeocoding();
            }
        }
    }
    
    private boolean isGeocodingEnabled() {
        return prefs.getBoolean(PREF_SHOW_SELF_ADDRESS, DEFAULT_SHOW_SELF_ADDRESS);
    }
    
    private int getRefreshPeriodSeconds() {
        try {
            String value = prefs.getString(PREF_REFRESH_PERIOD, String.valueOf(DEFAULT_REFRESH_PERIOD_SECONDS));
            int seconds = Integer.parseInt(value);
            return Math.max(1, seconds); // Minimum 1 second
        } catch (Exception e) {
            return DEFAULT_REFRESH_PERIOD_SECONDS;
        }
    }
    
    private void startGeocoding() {
        Log.d(TAG, "Starting geocoding with period: " + getRefreshPeriodSeconds() + "s");
        
        if (geocodingTask != null) {
            geocodingTask.cancel(false);
        }
        
        geocodingTask = scheduler.scheduleWithFixedDelay(
                this::performGeocoding,
                0, // Initial delay
                getRefreshPeriodSeconds(),
                TimeUnit.SECONDS
        );
    }
    
    private void stopGeocoding() {
        Log.d(TAG, "Stopping geocoding");
        
        if (geocodingTask != null) {
            geocodingTask.cancel(false);
            geocodingTask = null;
        }
    }
    
    private void performGeocoding() {
        if (mapView == null) return;
        
        // Get self marker position
        PointMapItem selfMarker = mapView.getSelfMarker();
        if (selfMarker == null) {
            return;
        }
        
        GeoPoint point = selfMarker.getPoint();
        if (point == null || !point.isValid()) {
            return;
        }
        
        // Skip if we haven't moved significantly (within threshold distance)
        if (lastGeocodedPoint != null) {
            double distance = point.distanceTo(lastGeocodedPoint);
            if (distance < MIN_DISTANCE_FOR_GEOCODE) {
                return;
            }
        }
        
        Log.d(TAG, "Geocoding location: " + point.getLatitude() + ", " + point.getLongitude());
        
        // Try ATAK's GeocodeManager first (better accuracy with house numbers)
        String formattedAddress = tryAtakGeocoder(point);
        
        // Fallback to Photon API if ATAK geocoder fails
        if (formattedAddress == null) {
            Log.d(TAG, "ATAK geocoder failed, falling back to Photon API");
            tryPhotonGeocoder(point);
            return; // Photon is async, will update widget when done
        }
        
        // ATAK geocoder succeeded
        lastGeocodedPoint = point;
        
        if (!formattedAddress.equals(currentAddress)) {
            Log.d(TAG, "Address changed, updating widget");
            currentAddress = formattedAddress;
            saveCachedAddress(currentAddress, point);
            updateWidget(currentAddress, COLOR_CYAN);
        }
    }
    
    /**
     * Try to geocode using ATAK's built-in GeocodeManager.
     * Returns the formatted address, or null if geocoding failed.
     */
    private String tryAtakGeocoder(GeoPoint point) {
        try {
            GeocodeManager geocodeManager = GeocodeManager.getInstance(mapView.getContext());
            GeocodeManager.Geocoder geocoder = geocodeManager.getSelectedGeocoder();
            
            if (geocoder == null) {
                Log.w(TAG, "No ATAK geocoder available");
                return null;
            }
            
            if (!geocoder.testServiceAvailable()) {
                Log.w(TAG, "ATAK geocoder service not available");
                return null;
            }
            
            // This call may block, but we're on a background thread
            List<Address> addresses = geocoder.getLocation(point);
            
            if (addresses == null || addresses.isEmpty()) {
                Log.w(TAG, "No addresses returned from ATAK geocoder");
                return null;
            }
            
            Address address = addresses.get(0);
            String formattedAddress = formatAddressFromAndroid(address);
            
            Log.d(TAG, "ATAK geocoding success: " + formattedAddress);
            return formattedAddress;
            
        } catch (Exception e) {
            Log.e(TAG, "ATAK geocoding error: " + e.getMessage(), e);
            return null;
        }
    }
    
    /**
     * Fallback: Try to geocode using Photon API (free OpenStreetMap-based service).
     * This is async and will update the widget when complete.
     */
    private void tryPhotonGeocoder(final GeoPoint point) {
        photonGeocoder.reverseGeocode(point.getLatitude(), point.getLongitude(),
                new ReverseGeocoder.ReverseGeocodeCallback() {
            @Override
            public void onSuccess(String address) {
                Log.d(TAG, "Photon geocoding success: " + address);
                lastGeocodedPoint = point;
                
                if (!address.equals(currentAddress)) {
                    currentAddress = address;
                    saveCachedAddress(currentAddress, point);
                    updateWidget(currentAddress, COLOR_CYAN);
                }
            }
            
            @Override
            public void onError(String errorMessage) {
                Log.w(TAG, "Photon geocoding failed: " + errorMessage);
                // Both geocoders failed - keep showing last known address
            }
        });
    }
    
    /**
     * Format an Android Address object for display.
     * Extracts street address with house number like the Kotlin code does.
     */
    private String formatAddressFromAndroid(Address address) {
        if (address == null) {
            return "Unknown location";
        }
        
        StringBuilder sb = new StringBuilder();
        
        // First try to get address lines (most complete)
        String line0 = address.getAddressLine(0);
        if (line0 != null && !line0.isEmpty()) {
            // Parse the address line - it typically contains "123 Street Name, City, State, Country"
            String[] parts = line0.split(",");
            
            // Take first part (street address with number)
            if (parts.length > 0) {
                sb.append(parts[0].trim());
            }
            
            // Add city if available (usually second part)
            if (parts.length > 1) {
                sb.append("\n").append(parts[1].trim());
            }
            
            // Add state/country in compact form
            if (parts.length > 2) {
                String lastPart = parts[parts.length - 1].trim();
                String countryCode = getCountryCode(lastPart);
                if (parts.length > 3) {
                    // Has state and country
                    String state = parts[parts.length - 2].trim();
                    sb.append("\n").append(state).append(", ").append(countryCode);
                } else {
                    sb.append("\n").append(countryCode);
                }
            }
            
            return sb.toString();
        }
        
        // Fallback: Build from individual components
        // Get street number + street name
        String subThoroughfare = address.getSubThoroughfare(); // House number
        String thoroughfare = address.getThoroughfare(); // Street name
        String featureName = address.getFeatureName(); // POI name or number
        
        Log.d(TAG, "Address components - subThoroughfare: " + subThoroughfare + 
                   ", thoroughfare: " + thoroughfare + ", featureName: " + featureName);
        
        // Build street address
        if (thoroughfare != null && !thoroughfare.isEmpty()) {
            if (subThoroughfare != null && !subThoroughfare.isEmpty()) {
                sb.append(subThoroughfare).append(" ");
            } else if (featureName != null && !featureName.isEmpty() && 
                       !featureName.equals(thoroughfare)) {
                // featureName might be the house number
                try {
                    Integer.parseInt(featureName);
                    sb.append(featureName).append(" ");
                } catch (NumberFormatException e) {
                    // Not a number, skip
                }
            }
            sb.append(thoroughfare);
        } else if (featureName != null && !featureName.isEmpty()) {
            sb.append(featureName);
        }
        
        // Add city
        String locality = address.getLocality();
        if (locality != null && !locality.isEmpty()) {
            if (sb.length() > 0) sb.append("\n");
            sb.append(locality);
        }
        
        // Add state/country
        String adminArea = address.getAdminArea();
        String countryCode = address.getCountryCode();
        
        if (adminArea != null || countryCode != null) {
            if (sb.length() > 0) sb.append("\n");
            if (adminArea != null && !adminArea.isEmpty()) {
                sb.append(adminArea);
                if (countryCode != null && !countryCode.isEmpty()) {
                    sb.append(", ").append(countryCode);
                }
            } else if (countryCode != null) {
                sb.append(countryCode);
            }
        }
        
        String result = sb.toString();
        return result.isEmpty() ? "Unknown location" : result;
    }
    
    /**
     * Load cached address from SharedPreferences.
     * Shows the last known address immediately while waiting for fresh geocoding.
     */
    private void loadCachedAddress() {
        String cachedAddress = prefs.getString(CACHE_LAST_ADDRESS, null);
        float cachedLat = prefs.getFloat(CACHE_LAST_LAT, 0f);
        float cachedLon = prefs.getFloat(CACHE_LAST_LON, 0f);
        
        if (cachedAddress != null && !cachedAddress.isEmpty()) {
            Log.d(TAG, "Loaded cached address: " + cachedAddress);
            currentAddress = cachedAddress;
            
            // Restore the last geocoded point to avoid unnecessary re-geocoding
            if (cachedLat != 0f || cachedLon != 0f) {
                lastGeocodedPoint = new GeoPoint(cachedLat, cachedLon);
            }
            
            // Show cached address immediately
            updateWidget(currentAddress, COLOR_CYAN);
        }
    }
    
    /**
     * Save address to cache for persistence across restarts.
     */
    private void saveCachedAddress(String address, GeoPoint point) {
        SharedPreferences.Editor editor = prefs.edit();
        editor.putString(CACHE_LAST_ADDRESS, address);
        if (point != null) {
            editor.putFloat(CACHE_LAST_LAT, (float) point.getLatitude());
            editor.putFloat(CACHE_LAST_LON, (float) point.getLongitude());
        }
        editor.apply();
        Log.d(TAG, "Cached address saved");
    }
    
    /**
     * Convert country name to 2-letter code.
     */
    private String getCountryCode(String countryName) {
        if (countryName == null) return "";
        
        // If already a 2-letter code, return as is
        if (countryName.length() == 2) return countryName.toUpperCase();
        
        String lower = countryName.toLowerCase().trim();
        switch (lower) {
            case "spain": case "españa": return "ES";
            case "united states": case "united states of america": case "usa": return "US";
            case "united kingdom": case "uk": case "great britain": return "UK";
            case "france": return "FR";
            case "germany": case "deutschland": return "DE";
            case "italy": case "italia": return "IT";
            case "portugal": return "PT";
            case "canada": return "CA";
            case "australia": return "AU";
            default:
                // Use first 2 letters as fallback
                return countryName.length() >= 2 ? countryName.substring(0, 2).toUpperCase() : countryName;
        }
    }
    
    private void updateWidget(String text, int color) {
        mainHandler.post(() -> {
            if (addressWidget != null) {
                // Only update if text or color has changed to avoid UI blinking
                if (text.equals(lastDisplayedText) && color == lastDisplayedColor) {
                    return;
                }
                lastDisplayedText = text;
                lastDisplayedColor = color;
                addressWidget.setText(text);
                addressWidget.setColor(color);
                updateWidgetVisibility(true);
            }
        });
    }
    
    private void updateWidgetVisibility(boolean visible) {
        mainHandler.post(() -> {
            if (addressWidget != null) {
                addressWidget.setVisible(visible);
            }
        });
    }
    
    /**
     * Public method to dispose the widget from outside.
     */
    public void dispose() {
        if (mapView != null && pluginContext != null) {
            onDestroyWidgets(pluginContext, mapView);
        }
    }
}
