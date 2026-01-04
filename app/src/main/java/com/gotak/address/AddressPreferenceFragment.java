package com.gotak.address;

import android.annotation.SuppressLint;
import android.content.Context;
import android.os.Bundle;

import com.atakmap.android.gui.PanPreference;
import com.atakmap.android.maps.MapView;
import com.atakmap.android.preference.PluginPreferenceFragment;
import com.gotak.address.plugin.R;
import com.gotak.address.search.SearchHistoryManager;

/**
 * Preference fragment for the Address Plugin settings.
 * Allows users to toggle the self-location address display and configure search options.
 */
public class AddressPreferenceFragment extends PluginPreferenceFragment {

    private static final String TAG = "AddressPreferenceFragment";

    @SuppressLint("StaticFieldLeak")
    private Context pluginContext;

    public AddressPreferenceFragment() {
        super(MapView.getMapView().getContext(), R.xml.preferences);
    }

    @SuppressLint("ValidFragment")
    public AddressPreferenceFragment(Context pluginContext) {
        super(pluginContext, R.xml.preferences);
        this.pluginContext = pluginContext;
    }

    @Override
    public void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);

        // Set up clear history button
        PanPreference clearHistoryPref = (PanPreference) findPreference("address_clear_history");
        if (clearHistoryPref != null) {
            clearHistoryPref.setOnPreferenceClickListener(preference -> {
                Context ctx = getActivity();
                if (ctx != null) {
                    SearchHistoryManager historyManager = new SearchHistoryManager(ctx);
                    historyManager.clearHistory();
                    android.widget.Toast.makeText(ctx, "Search history cleared", 
                            android.widget.Toast.LENGTH_SHORT).show();
                }
                return true;
            });
        }
    }

    @Override
    public String getSubTitle() {
        return "Address geocoding and search settings";
    }
}

