
package com.gotak.address;

import android.content.Context;
import android.content.Intent;

import com.atakmap.android.dropdown.DropDownMapComponent;
import com.atakmap.app.preferences.ToolsPreferenceFragment;
import com.gotak.address.plugin.R;
import com.gotak.address.search.AddressSearchDropDown;
import com.gotak.address.search.OfflineDataDropDown;
import com.gotak.address.search.SearchButtonWidget;
import com.atakmap.android.ipc.AtakBroadcast.DocumentedIntentFilter;
import com.atakmap.android.maps.MapView;
import com.atakmap.coremap.log.Log;

/**
 * Address Plugin Map Component
 * Provides address search and self-location geocoding functionality.
 */
public class AddressMapComponent extends DropDownMapComponent {

    public static final String TAG = "AddressMapComponent";

    private Context pluginContext;

    // Address search components
    private SearchButtonWidget searchButtonWidget;
    private AddressSearchDropDown addressSearchDropDown;
    
    // Offline data manager dropdown
    private OfflineDataDropDown offlineDataDropDown;
    
    // Self location geocoding widget
    private com.gotak.address.selfgeo.SelfLocationWidget selfLocationWidget;
    
    // Map center crosshairs geocoding widget
    private com.gotak.address.selfgeo.MapCenterWidget mapCenterWidget;
    
    // Marker selection geocoding widget (top right)
    private com.gotak.address.selfgeo.MarkerSelectionWidget markerSelectionWidget;

    @Override
    public void onCreate(final Context context, Intent intent,
            final MapView view) {

        // Set the theme to match ATAK
        context.setTheme(R.style.ATAKPluginTheme);

        super.onCreate(context, intent, view);
        pluginContext = context;

        // Register preferences in ATAK settings menu
        ToolsPreferenceFragment.register(
                new ToolsPreferenceFragment.ToolPreference(
                        context.getString(R.string.address_plugin_preferences),
                        "Address geocoding and search settings",
                        "addressPreferences",
                        context.getResources().getDrawable(R.drawable.ic_launcher, null),
                        new AddressPreferenceFragment(context)));

        // Initialize Address Search components
        try {
            Log.d(TAG, "Initializing address search components");
            
            // Create and register the search dropdown receiver
            addressSearchDropDown = new AddressSearchDropDown(view, pluginContext);
            DocumentedIntentFilter searchFilter = new DocumentedIntentFilter();
            searchFilter.addAction(AddressSearchDropDown.SHOW_SEARCH,
                    "Show the address search panel");
            searchFilter.addAction(AddressSearchDropDown.HIDE_SEARCH,
                    "Hide the address search panel");
            searchFilter.addAction(AddressSearchDropDown.NAVIGATE_TO_VIEW,
                    "Navigate to a saved view by ID or name. Extras: view_id (String) or view_name (String)");
            searchFilter.addAction(AddressSearchDropDown.NAVIGATE_TO_POSITION,
                    "Navigate to arbitrary map position. Required: latitude (double), longitude (double). " +
                    "Optional: scale (double), zoom (double), tilt (double), rotation (double), altitude (double)");
            this.registerDropDownReceiver(addressSearchDropDown, searchFilter);
            
            // Refresh POI icons after ATAK restart (delay to ensure map is ready)
            view.postDelayed(() -> {
                if (addressSearchDropDown != null) {
                    addressSearchDropDown.refreshPoiIcons();
                }
            }, 2000);
            
            // Create and register the offline data dropdown receiver
            offlineDataDropDown = new OfflineDataDropDown(view, pluginContext);
            DocumentedIntentFilter offlineFilter = new DocumentedIntentFilter();
            offlineFilter.addAction(OfflineDataDropDown.SHOW_OFFLINE_DATA,
                    "Show the offline data manager");
            offlineFilter.addAction(OfflineDataDropDown.HIDE_OFFLINE_DATA,
                    "Hide the offline data manager");
            this.registerDropDownReceiver(offlineDataDropDown, offlineFilter);
            
            // Create the search button widget
            searchButtonWidget = new SearchButtonWidget();
            searchButtonWidget.onCreate(context, intent, view);
            
            // Create the self location widget (shows address above callsign)
            selfLocationWidget = new com.gotak.address.selfgeo.SelfLocationWidget();
            selfLocationWidget.onCreate(pluginContext, intent, view);
            
            // Create the map center widget (shows address for crosshairs)
            mapCenterWidget = new com.gotak.address.selfgeo.MapCenterWidget();
            mapCenterWidget.onCreate(pluginContext, intent, view);
            
            // Create the marker selection widget (shows address for tapped markers)
            markerSelectionWidget = new com.gotak.address.selfgeo.MarkerSelectionWidget();
            markerSelectionWidget.onCreate(pluginContext, intent, view);
            
            Log.d(TAG, "Address search components initialized");
        } catch (Exception e) {
            Log.e(TAG, "Failed to initialize address search components: " + e.getMessage(), e);
        }
    }

    @Override
    protected void onDestroyImpl(Context context, MapView view) {
        Log.d(TAG, "calling on destroy");
        
        // Unregister preferences
        ToolsPreferenceFragment.unregister("addressPreferences");
        
        // Clean up address search components
        try {
            if (searchButtonWidget != null) {
                searchButtonWidget.dispose();
                searchButtonWidget = null;
            }
            if (addressSearchDropDown != null) {
                addressSearchDropDown.dispose();
                addressSearchDropDown = null;
            }
            if (offlineDataDropDown != null) {
                offlineDataDropDown.dispose();
                offlineDataDropDown = null;
            }
            if (selfLocationWidget != null) {
                selfLocationWidget.dispose();
                selfLocationWidget = null;
            }
            if (mapCenterWidget != null) {
                mapCenterWidget.dispose();
                mapCenterWidget = null;
            }
            if (markerSelectionWidget != null) {
                markerSelectionWidget.dispose();
                markerSelectionWidget = null;
            }
        } catch (Exception e) {
            Log.e(TAG, "Error cleaning up address search components: " + e.getMessage(), e);
        }
        
        super.onDestroyImpl(context, view);
    }
}
