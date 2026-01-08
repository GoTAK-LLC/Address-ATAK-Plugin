package com.gotak.address.search;

import android.content.Context;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageButton;
import android.widget.ImageView;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;

import com.gotak.address.plugin.R;

import java.util.ArrayList;
import java.util.List;

/**
 * RecyclerView adapter for displaying Nominatim search results.
 */
public class SearchResultsAdapter extends RecyclerView.Adapter<SearchResultsAdapter.ViewHolder> {

    private final List<NominatimSearchResult> results = new ArrayList<>();
    private final OnResultClickListener listener;
    private final Context context;

    /**
     * Interface for handling result item clicks.
     */
    public interface OnResultClickListener {
        void onResultClick(NominatimSearchResult result);
        void onResultDropMarker(NominatimSearchResult result);
        void onResultNavigate(NominatimSearchResult result);
    }

    public SearchResultsAdapter(Context context, OnResultClickListener listener) {
        this.context = context;
        this.listener = listener;
    }

    /**
     * Update the list of results displayed.
     */
    public void setResults(List<NominatimSearchResult> newResults) {
        results.clear();
        if (newResults != null) {
            results.addAll(newResults);
        }
        notifyDataSetChanged();
    }

    /**
     * Clear all results.
     */
    public void clear() {
        results.clear();
        notifyDataSetChanged();
    }

    @NonNull
    @Override
    public ViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        View view = LayoutInflater.from(context)
                .inflate(R.layout.address_search_result_item, parent, false);
        return new ViewHolder(view);
    }

    @Override
    public void onBindViewHolder(@NonNull ViewHolder holder, int position) {
        NominatimSearchResult result = results.get(position);
        holder.bind(result, listener);
    }

    @Override
    public int getItemCount() {
        return results.size();
    }

    /**
     * ViewHolder for search result items.
     */
    static class ViewHolder extends RecyclerView.ViewHolder {
        private final View clickableArea;
        private final ImageView iconView;
        private final TextView nameText;
        private final TextView addressText;
        private final ImageButton dropMarkerButton;
        private final ImageButton navigateButton;

        ViewHolder(@NonNull View itemView) {
            super(itemView);
            clickableArea = itemView.findViewById(R.id.result_item_clickable);
            iconView = itemView.findViewById(R.id.result_icon);
            nameText = itemView.findViewById(R.id.result_name);
            addressText = itemView.findViewById(R.id.result_address);
            dropMarkerButton = itemView.findViewById(R.id.result_drop_marker);
            navigateButton = itemView.findViewById(R.id.result_navigate);
        }

        void bind(NominatimSearchResult result, OnResultClickListener listener) {
            nameText.setText(result.getName());
            addressText.setText(result.getDisplayName());

            // Set icon based on location type
            LocationType locationType = LocationType.fromResult(result);
            iconView.setImageResource(locationType.getIconRes());

            clickableArea.setOnClickListener(v -> {
                if (listener != null) {
                    listener.onResultClick(result);
                }
            });

            dropMarkerButton.setOnClickListener(v -> {
                if (listener != null) {
                    listener.onResultDropMarker(result);
                }
            });

            navigateButton.setOnClickListener(v -> {
                if (listener != null) {
                    listener.onResultNavigate(result);
                }
            });
        }
    }
}

