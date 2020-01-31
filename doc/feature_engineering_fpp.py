# ts_arr: contains a list of time stamps in sorted order seen for a given entity
# all_vals_arr: contains the list of values seen at the respective time stamps in ts_arr
# window_h: is size of window in hours to look back from the current time point
# all_weights_arr: contains the weigths obtained from conifdence features for the respective timestamps
# weights_idx_array: tells about the position of the 3 location features where we need to apply the weights obtained from confidence features
def apply_group_window_func_isin(ts_arr, all_vals_arr, window_h, all_weights_array, weights_idx_array):
    N_features = all_vals_arr.shape[1]
    N_ts = ts_arr.shape[0]
    stats_arr = np.zeros((N_ts, N_features))
    stats_arr[:] = -0.2
    
    for i in prange(N_ts):
        t = ts_arr[i]
        # The window for %age calculation should not include the current value
        idx_arr = win_idx(t, ts_arr, window_h, False)
        idx_len = idx_arr.shape[0]
        
        # Compute values per feature
        for bf in range(N_features):
            vals_arr = all_vals_arr[idx_arr, bf]
            v = all_vals_arr[i, bf]
            
            # If no weights present then count each entry with weight 1
            if (idx_len > 0) and (np.any(weights_idx_array == bf)):
                cf = np.where(weights_idx_array == bf)[0][0]
                weights_array = all_weights_array[idx_arr, cf]
                weights_array[np.isnan(weights_array)] = 1.0
                stats_arr[i, bf] = np.sum(weights_array[(vals_arr == v)]) * 1.0 / idx_len # 1 * Weight
            elif (idx_len > 0):
                stats_arr[i, bf] = np.sum(vals_arr == v) * 1.0 / idx_len
    return stats_arr.T
 
# ts_arr: contains a list of time stamps in sorted order seen for a given entity
# all_vals_arr: contains the list of values seen at the respective time stamps in ts_arr
# window_h: is size of window in hours to look back from the current time point
# all_weights_arr: empty array here
# weights_idx_array: empty array here
def apply_group_window_func_unique(ts_arr, all_vals_arr, window_h, all_weights_array, weights_idx_array):
    N_features = all_vals_arr.shape[1]
    N_ts = ts_arr.shape[0]
    stats_arr = np.zeros((N_ts, N_features), dtype=np.int32)
    
    for i in prange(N_ts):
        t = ts_arr[i]
        # The window for %age calculation should not include the current value
        idx_arr = win_idx(t, ts_arr, window_h, True)
        idx_len = idx_arr.shape[0]
        
        # Compute values per feature
        for bf in range(N_features):
            vals_arr = all_vals_arr[idx_arr, bf]
            if(idx_len > 0):
                stats_arr[i, bf] = np.unique(vals_arr).shape[0]
            
    return stats_arr.T
