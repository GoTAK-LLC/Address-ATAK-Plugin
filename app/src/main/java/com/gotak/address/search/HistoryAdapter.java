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
 * RecyclerView adapter for displaying search history with remove buttons.
 */
public class HistoryAdapter extends RecyclerView.Adapter<HistoryAdapter.ViewHolder> {

    private final List<NominatimSearchResult> items = new ArrayList<>();
    private final HistoryItemListener listener;
    private final Context context;

    /**
     * Interface for handling history item interactions.
     */
    public interface HistoryItemListener {
        void onHistoryItemClick(NominatimSearchResult result);
        void onHistoryItemRemove(NominatimSearchResult result);
        void onHistoryItemNavigate(NominatimSearchResult result);
        void onHistoryItemDropMarker(NominatimSearchResult result);
    }

    public HistoryAdapter(Context context, HistoryItemListener listener) {
        this.context = context;
        this.listener = listener;
    }

    /**
     * Update the list of history items.
     */
    public void setItems(List<NominatimSearchResult> newItems) {
        items.clear();
        if (newItems != null) {
            items.addAll(newItems);
        }
        notifyDataSetChanged();
    }

    /**
     * Clear all items.
     */
    public void clear() {
        items.clear();
        notifyDataSetChanged();
    }

    @NonNull
    @Override
    public ViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        View view = LayoutInflater.from(context)
                .inflate(R.layout.address_history_item, parent, false);
        return new ViewHolder(view);
    }

    @Override
    public void onBindViewHolder(@NonNull ViewHolder holder, int position) {
        NominatimSearchResult item = items.get(position);
        holder.bind(item, listener);
    }

    @Override
    public int getItemCount() {
        return items.size();
    }

    /**
     * ViewHolder for history items.
     */
    static class ViewHolder extends RecyclerView.ViewHolder {
        private final View clickableArea;
        private final ImageView iconView;
        private final TextView nameText;
        private final TextView addressText;
        private final ImageButton dropMarkerButton;
        private final ImageButton navigateButton;
        private final ImageButton removeButton;

        ViewHolder(@NonNull View itemView) {
            super(itemView);
            clickableArea = itemView.findViewById(R.id.history_item_clickable);
            iconView = itemView.findViewById(R.id.history_icon);
            nameText = itemView.findViewById(R.id.history_name);
            addressText = itemView.findViewById(R.id.history_address);
            dropMarkerButton = itemView.findViewById(R.id.history_drop_marker);
            navigateButton = itemView.findViewById(R.id.history_navigate);
            removeButton = itemView.findViewById(R.id.history_remove);
        }

        void bind(NominatimSearchResult item, HistoryItemListener listener) {
            nameText.setText(item.getName());
            addressText.setText(item.getDisplayName());

            // Set icon based on location type
            LocationType locationType = LocationType.fromResult(item);
            iconView.setImageResource(locationType.getIconRes());

            clickableArea.setOnClickListener(v -> {
                if (listener != null) {
                    listener.onHistoryItemClick(item);
                }
            });

            dropMarkerButton.setOnClickListener(v -> {
                if (listener != null) {
                    listener.onHistoryItemDropMarker(item);
                }
            });

            navigateButton.setOnClickListener(v -> {
                if (listener != null) {
                    listener.onHistoryItemNavigate(item);
                }
            });

            removeButton.setOnClickListener(v -> {
                if (listener != null) {
                    listener.onHistoryItemRemove(item);
                }
            });
        }
    }
}

