package com.gotak.address.search;

import android.content.Context;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.ImageView;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;

import com.gotak.address.plugin.R;

import java.util.ArrayList;
import java.util.List;

/**
 * Adapter for displaying available states for offline download.
 */
public class OfflineStateAdapter extends RecyclerView.Adapter<OfflineStateAdapter.ViewHolder> {

    public interface StateActionListener {
        void onDownload(OfflineDataManager.StateInfo state);
        void onDelete(OfflineDataManager.StateInfo state);
    }

    private final Context context;
    private final StateActionListener listener;
    private final List<OfflineDataManager.StateInfo> states = new ArrayList<>();

    public OfflineStateAdapter(Context context, StateActionListener listener) {
        this.context = context;
        this.listener = listener;
    }

    public void setStates(List<OfflineDataManager.StateInfo> newStates) {
        states.clear();
        states.addAll(newStates);
        
        // Sort: downloaded items first, then alphabetically by name
        states.sort((a, b) -> {
            if (a.downloaded != b.downloaded) {
                return a.downloaded ? -1 : 1;  // Downloaded first
            }
            return a.name.compareToIgnoreCase(b.name);
        });
        
        notifyDataSetChanged();
    }

    public void updateState(OfflineDataManager.StateInfo updatedState) {
        for (int i = 0; i < states.size(); i++) {
            if (states.get(i).id.equals(updatedState.id)) {
                states.set(i, updatedState);
                notifyItemChanged(i);
                break;
            }
        }
    }

    @NonNull
    @Override
    public ViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        View view = LayoutInflater.from(context)
                .inflate(R.layout.offline_state_item, parent, false);
        return new ViewHolder(view);
    }

    @Override
    public void onBindViewHolder(@NonNull ViewHolder holder, int position) {
        OfflineDataManager.StateInfo state = states.get(position);
        holder.bind(state);
    }

    @Override
    public int getItemCount() {
        return states.size();
    }

    class ViewHolder extends RecyclerView.ViewHolder {
        private final TextView stateName;
        private final TextView stateAbbrev;
        private final TextView stateInfo;
        private final ImageView downloadedIcon;
        private final Button actionButton;

        ViewHolder(View itemView) {
            super(itemView);
            stateName = itemView.findViewById(R.id.state_name);
            stateAbbrev = itemView.findViewById(R.id.state_abbrev);
            stateInfo = itemView.findViewById(R.id.state_info);
            downloadedIcon = itemView.findViewById(R.id.downloaded_icon);
            actionButton = itemView.findViewById(R.id.action_button);
        }

        void bind(OfflineDataManager.StateInfo state) {
            // Show full name only (no abbreviation clutter)
            stateName.setText(state.name);
            
            // Only show abbreviation if it's meaningful (not empty, not same as name)
            if (state.abbrev != null && !state.abbrev.isEmpty() && 
                !state.abbrev.equalsIgnoreCase(state.name)) {
                stateAbbrev.setText("(" + state.abbrev + ")");
                stateAbbrev.setVisibility(View.VISIBLE);
            } else {
                stateAbbrev.setVisibility(View.GONE);
            }
            
            // Show size and place count
            StringBuilder info = new StringBuilder();
            if (state.downloaded) {
                info.append("✓ DOWNLOADED • ");
            }
            info.append(state.getSizeFormatted());
            if (state.placeCount > 0) {
                info.append(" • ").append(state.getPlaceCountFormatted());
            }
            stateInfo.setText(info.toString());

            // Update UI based on downloaded state
            if (state.downloaded) {
                downloadedIcon.setVisibility(View.VISIBLE);
                actionButton.setText("Delete");
                actionButton.setBackgroundResource(R.drawable.bg_button_delete);
                actionButton.setOnClickListener(v -> {
                    if (listener != null) {
                        listener.onDelete(state);
                    }
                });
            } else {
                downloadedIcon.setVisibility(View.GONE);
                actionButton.setText("Download");
                actionButton.setBackgroundResource(R.drawable.bg_button_download);
                actionButton.setOnClickListener(v -> {
                    if (listener != null) {
                        listener.onDownload(state);
                    }
                });
            }
        }
    }
}

