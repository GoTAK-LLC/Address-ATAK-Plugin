package com.gotak.address.search;

import android.content.Context;
import android.net.ConnectivityManager;
import android.net.NetworkInfo;
import android.os.Handler;
import android.os.Looper;

import com.atakmap.coremap.log.Log;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * HTTP client for geocoding APIs with fuzzy search support.
 * 
 * Search priority:
 * 1. Offline databases (if downloaded) - instant results, works without internet
 * 2. Photon API (https://photon.komoot.io/) - Built on OSM data with typo tolerance
 * 3. Nominatim API (https://nominatim.openstreetmap.org/) - Standard OSM geocoder
 * 
 * Photon provides fuzzy matching so "ontigol" will find "Ontigola", etc.
 * No API key required for either service.
 */
public class NominatimApiClient {
    private static final String TAG = "NominatimApiClient";
    
    // Photon API - has built-in fuzzy/typo-tolerant search
    private static final String PHOTON_URL = "https://photon.komoot.io/api/";
    
    // Nominatim API - fallback if Photon fails
    private static final String NOMINATIM_URL = "https://nominatim.openstreetmap.org/search";
    
    private static final String USER_AGENT = "ATAK-AddressPlugin/1.0";
    private static final int CONNECT_TIMEOUT = 10000; // 10 seconds
    private static final int READ_TIMEOUT = 15000; // 15 seconds
    private static final int DEFAULT_LIMIT = 10;

    private final ExecutorService executor;
    private final Handler mainHandler;
    
    // Offline database support
    private Context context;
    private OfflineAddressDatabase offlineDatabase;
    private boolean offlineOnly = false;

    public NominatimApiClient() {
        this.executor = Executors.newSingleThreadExecutor();
        this.mainHandler = new Handler(Looper.getMainLooper());
    }
    
    /**
     * Initialize with context for offline database support.
     */
    public NominatimApiClient(Context context) {
        this();
        this.context = context;
        this.offlineDatabase = new OfflineAddressDatabase(context);
    }
    
    /**
     * Set whether to use offline-only mode (no network requests).
     */
    public void setOfflineOnly(boolean offlineOnly) {
        this.offlineOnly = offlineOnly;
    }
    
    /**
     * Get the offline database instance.
     */
    public OfflineAddressDatabase getOfflineDatabase() {
        return offlineDatabase;
    }
    
    /**
     * Check if device has network connectivity.
     */
    private boolean isNetworkAvailable() {
        if (context == null) return true; // Assume available if no context
        
        try {
            ConnectivityManager cm = (ConnectivityManager) 
                    context.getSystemService(Context.CONNECTIVITY_SERVICE);
            if (cm != null) {
                NetworkInfo activeNetwork = cm.getActiveNetworkInfo();
                return activeNetwork != null && activeNetwork.isConnected();
            }
        } catch (Exception e) {
            Log.w(TAG, "Error checking network: " + e.getMessage());
        }
        return true; // Assume available on error
    }

    /**
     * Callback interface for search results.
     */
    public interface SearchCallback {
        void onSuccess(List<NominatimSearchResult> results);
        void onError(String errorMessage);
    }

    /**
     * Search for places matching the query with fuzzy matching.
     * Runs on a background thread and returns results via callback on the main thread.
     * 
     * Search priority:
     * 1. Offline databases (instant, works without internet)
     * 2. Online APIs (Photon, then Nominatim as fallback)
     */
    public void search(String query, SearchCallback callback) {
        executor.execute(() -> {
            List<NominatimSearchResult> results = new ArrayList<>();
            
            // Step 1: Try offline database first (instant results)
            if (offlineDatabase != null && !offlineDatabase.getDownloadedStates().isEmpty()) {
                try {
                    Log.d(TAG, "Searching offline databases...");
                    results = offlineDatabase.searchAllStates(query);
                    Log.i(TAG, "Offline search found " + results.size() + " results");
                    
                    // If offline-only mode or we have good results, return them
                    if (offlineOnly || results.size() >= DEFAULT_LIMIT) {
                        final List<NominatimSearchResult> finalResults = results;
                        mainHandler.post(() -> callback.onSuccess(finalResults));
                        return;
                    }
                } catch (Exception e) {
                    Log.w(TAG, "Offline search error: " + e.getMessage());
                }
            }
            
            // Step 2: If offline-only mode, return what we have
            if (offlineOnly) {
                final List<NominatimSearchResult> finalResults = results;
                mainHandler.post(() -> callback.onSuccess(finalResults));
                return;
            }
            
            // Step 3: Check network availability
            if (!isNetworkAvailable()) {
                Log.w(TAG, "No network available, returning offline results only");
                final List<NominatimSearchResult> finalResults = results;
                if (results.isEmpty()) {
                    mainHandler.post(() -> callback.onError("No network connection and no offline data"));
                } else {
                    mainHandler.post(() -> callback.onSuccess(finalResults));
                }
                return;
            }
            
            // Step 4: Try online APIs
            try {
                // Try Photon first (better fuzzy matching)
                List<NominatimSearchResult> onlineResults = performPhotonSearch(query);
                
                // If Photon returns no results, try Nominatim as fallback
                if (onlineResults.isEmpty()) {
                    Log.d(TAG, "Photon returned no results, trying Nominatim fallback");
                    onlineResults = performNominatimSearch(query);
                }
                
                // Merge with offline results (prefer online for fresher data)
                if (!onlineResults.isEmpty()) {
                    results = onlineResults;
                }
                
                final List<NominatimSearchResult> finalResults = results;
                mainHandler.post(() -> callback.onSuccess(finalResults));
            } catch (Exception e) {
                Log.e(TAG, "Online search error: " + e.getMessage(), e);
                
                // Try Nominatim as fallback on any error
                try {
                    List<NominatimSearchResult> nominatimResults = performNominatimSearch(query);
                    if (!nominatimResults.isEmpty()) {
                        results = nominatimResults;
                    }
                    final List<NominatimSearchResult> finalResults = results;
                    mainHandler.post(() -> callback.onSuccess(finalResults));
                } catch (Exception e2) {
                    Log.e(TAG, "Fallback search also failed: " + e2.getMessage(), e2);
                    // Return offline results if we have any
                    if (!results.isEmpty()) {
                        final List<NominatimSearchResult> finalResults = results;
                        mainHandler.post(() -> callback.onSuccess(finalResults));
                    } else {
                        mainHandler.post(() -> callback.onError(e.getMessage()));
                    }
                }
            }
        });
    }

    /**
     * Perform Photon API search - has built-in fuzzy/typo-tolerant matching.
     */
    private List<NominatimSearchResult> performPhotonSearch(String query) throws IOException, JSONException {
        String encodedQuery = URLEncoder.encode(query, "UTF-8");
        String urlString = PHOTON_URL + "?q=" + encodedQuery + "&limit=" + DEFAULT_LIMIT;

        Log.d(TAG, "Photon search: " + urlString);

        HttpURLConnection connection = null;
        try {
            URL url = new URL(urlString);
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setRequestProperty("User-Agent", USER_AGENT);
            connection.setRequestProperty("Accept", "application/json");
            connection.setConnectTimeout(CONNECT_TIMEOUT);
            connection.setReadTimeout(READ_TIMEOUT);

            int responseCode = connection.getResponseCode();
            Log.d(TAG, "Photon response code: " + responseCode);

            if (responseCode != HttpURLConnection.HTTP_OK) {
                throw new IOException("Photon HTTP error: " + responseCode);
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

            // Parse GeoJSON response from Photon
            JSONObject jsonResponse = new JSONObject(response.toString());
            JSONArray features = jsonResponse.getJSONArray("features");
            List<NominatimSearchResult> results = new ArrayList<>();
            
            for (int i = 0; i < features.length(); i++) {
                NominatimSearchResult result = parsePhotonFeature(features.getJSONObject(i));
                if (result != null) {
                    results.add(result);
                }
            }

            Log.i(TAG, "Photon found " + results.size() + " results for: " + query);
            return results;

        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    /**
     * Parse a Photon GeoJSON feature into our result format.
     */
    private NominatimSearchResult parsePhotonFeature(JSONObject feature) {
        try {
            JSONObject geometry = feature.getJSONObject("geometry");
            JSONArray coordinates = geometry.getJSONArray("coordinates");
            double longitude = coordinates.getDouble(0);
            double latitude = coordinates.getDouble(1);

            JSONObject properties = feature.getJSONObject("properties");
            
            long osmId = properties.optLong("osm_id", 0);
            String osmType = properties.optString("osm_type", null);
            String name = properties.optString("name", null);
            String type = properties.optString("type", null);
            
            // Build display name from address components
            String displayName = buildDisplayName(properties);
            
            // Use OSM ID as place ID (Photon doesn't have separate place_id)
            long placeId = osmId;

            return new NominatimSearchResult(placeId, latitude, longitude,
                    displayName, name, type, osmType, osmId);
        } catch (JSONException e) {
            Log.e(TAG, "Error parsing Photon feature", e);
            return null;
        }
    }

    /**
     * Build a readable display name from Photon address properties.
     */
    private String buildDisplayName(JSONObject properties) {
        StringBuilder sb = new StringBuilder();
        
        String name = properties.optString("name", null);
        String street = properties.optString("street", null);
        String housenumber = properties.optString("housenumber", null);
        String city = properties.optString("city", null);
        String state = properties.optString("state", null);
        String country = properties.optString("country", null);
        String postcode = properties.optString("postcode", null);
        
        // Add name first if available
        if (name != null && !name.isEmpty()) {
            sb.append(name);
        }
        
        // Add street address
        if (street != null && !street.isEmpty()) {
            if (sb.length() > 0) sb.append(", ");
            if (housenumber != null && !housenumber.isEmpty()) {
                sb.append(housenumber).append(" ");
            }
            sb.append(street);
        }
        
        // Add city
        if (city != null && !city.isEmpty()) {
            if (sb.length() > 0) sb.append(", ");
            sb.append(city);
        }
        
        // Add state
        if (state != null && !state.isEmpty()) {
            if (sb.length() > 0) sb.append(", ");
            sb.append(state);
        }
        
        // Add postcode
        if (postcode != null && !postcode.isEmpty()) {
            if (sb.length() > 0) sb.append(" ");
            sb.append(postcode);
        }
        
        // Add country
        if (country != null && !country.isEmpty()) {
            if (sb.length() > 0) sb.append(", ");
            sb.append(country);
        }
        
        return sb.length() > 0 ? sb.toString() : "Unknown location";
    }

    /**
     * Perform Nominatim search as fallback (less fuzzy but more comprehensive).
     */
    private List<NominatimSearchResult> performNominatimSearch(String query) throws IOException, JSONException {
        String encodedQuery = URLEncoder.encode(query, "UTF-8");
        String urlString = NOMINATIM_URL + "?q=" + encodedQuery 
                + "&format=json"
                + "&addressdetails=1"
                + "&limit=" + DEFAULT_LIMIT;

        Log.d(TAG, "Nominatim search: " + urlString);

        HttpURLConnection connection = null;
        try {
            URL url = new URL(urlString);
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setRequestProperty("User-Agent", USER_AGENT);
            connection.setRequestProperty("Accept", "application/json");
            connection.setConnectTimeout(CONNECT_TIMEOUT);
            connection.setReadTimeout(READ_TIMEOUT);

            int responseCode = connection.getResponseCode();
            Log.d(TAG, "Nominatim response code: " + responseCode);

            if (responseCode != HttpURLConnection.HTTP_OK) {
                throw new IOException("Nominatim HTTP error: " + responseCode);
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

            // Parse JSON array of results
            JSONArray jsonArray = new JSONArray(response.toString());
            List<NominatimSearchResult> results = new ArrayList<>();
            
            for (int i = 0; i < jsonArray.length(); i++) {
                NominatimSearchResult result = NominatimSearchResult.fromJson(
                        jsonArray.getJSONObject(i));
                results.add(result);
            }

            Log.i(TAG, "Nominatim found " + results.size() + " results for: " + query);
            return results;

        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    /**
     * Shutdown the executor service and close offline database.
     */
    public void shutdown() {
        executor.shutdown();
        if (offlineDatabase != null) {
            offlineDatabase.close();
        }
    }
}

