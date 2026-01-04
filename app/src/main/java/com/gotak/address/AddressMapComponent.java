
package com.gotak.address;

import android.content.Context;
import android.content.Intent;

import com.atakmap.android.dropdown.DropDownMapComponent;
import com.atakmap.app.preferences.ToolsPreferenceFragment;
import com.gotak.address.plugin.R;
import com.gotak.address.search.AddressSearchDropDown;
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
    
    // Self location geocoding widget
    private com.gotak.address.selfgeo.SelfLocationWidget selfLocationWidget;

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
            this.registerDropDownReceiver(addressSearchDropDown, searchFilter);
            
            // Create the search button widget
            searchButtonWidget = new SearchButtonWidget();
            searchButtonWidget.onCreate(context, intent, view);
            
            // Create the self location widget (shows address above callsign)
            selfLocationWidget = new com.gotak.address.selfgeo.SelfLocationWidget();
            selfLocationWidget.onCreate(pluginContext, intent, view);
            
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
            if (selfLocationWidget != null) {
                selfLocationWidget.dispose();
                selfLocationWidget = null;
            }
        } catch (Exception e) {
            Log.e(TAG, "Error cleaning up address search components: " + e.getMessage(), e);
        }
        
        super.onDestroyImpl(context, view);
    }
}
