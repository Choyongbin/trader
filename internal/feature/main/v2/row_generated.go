// Code generated for the frozen Feature V2 registry. DO NOT EDIT.
package featurev2

type FeatureRowV2 struct {
	DecisionTimestampMs int64   `parquet:"decision_timestamp_ms"`
	F000                float64 `parquet:"bar_log_return"`
	F001                float64 `parquet:"bar_range"`
	F002                float64 `parquet:"bar_vwap_deviation"`
	F003                float64 `parquet:"ret_log_1s"`
	F004                float64 `parquet:"ret_log_3s"`
	F005                float64 `parquet:"ret_log_5s"`
	F006                float64 `parquet:"ret_log_10s"`
	F007                float64 `parquet:"ret_log_30s"`
	F008                float64 `parquet:"ret_log_60s"`
	F009                float64 `parquet:"ret_log_180s"`
	F010                float64 `parquet:"ret_log_300s"`
	F011                float64 `parquet:"ret_log_900s"`
	F012                float64 `parquet:"ret_log_1800s"`
	F013                float64 `parquet:"ret_log_3600s"`
	F014                float64 `parquet:"ret_log_14400s"`
	F015                float64 `parquet:"range_width_10s"`
	F016                float64 `parquet:"range_position_10s"`
	F017                float64 `parquet:"range_width_30s"`
	F018                float64 `parquet:"range_position_30s"`
	F019                float64 `parquet:"range_width_60s"`
	F020                float64 `parquet:"range_position_60s"`
	F021                float64 `parquet:"range_width_300s"`
	F022                float64 `parquet:"range_position_300s"`
	F023                float64 `parquet:"range_width_900s"`
	F024                float64 `parquet:"range_position_900s"`
	F025                float64 `parquet:"range_width_3600s"`
	F026                float64 `parquet:"range_position_3600s"`
	F027                float64 `parquet:"range_width_14400s"`
	F028                float64 `parquet:"range_position_14400s"`
	F029                float64 `parquet:"rv_10s"`
	F030                float64 `parquet:"rv_30s"`
	F031                float64 `parquet:"rv_60s"`
	F032                float64 `parquet:"rv_300s"`
	F033                float64 `parquet:"rv_900s"`
	F034                float64 `parquet:"rv_3600s"`
	F035                float64 `parquet:"rv_14400s"`
	F036                float64 `parquet:"base_volume_sum_5s"`
	F037                float64 `parquet:"quote_volume_sum_5s"`
	F038                float64 `parquet:"agg_trade_count_sum_5s"`
	F039                float64 `parquet:"base_volume_sum_30s"`
	F040                float64 `parquet:"quote_volume_sum_30s"`
	F041                float64 `parquet:"agg_trade_count_sum_30s"`
	F042                float64 `parquet:"base_volume_sum_60s"`
	F043                float64 `parquet:"quote_volume_sum_60s"`
	F044                float64 `parquet:"agg_trade_count_sum_60s"`
	F045                float64 `parquet:"base_volume_sum_300s"`
	F046                float64 `parquet:"quote_volume_sum_300s"`
	F047                float64 `parquet:"agg_trade_count_sum_300s"`
	F048                float64 `parquet:"base_volume_sum_900s"`
	F049                float64 `parquet:"quote_volume_sum_900s"`
	F050                float64 `parquet:"agg_trade_count_sum_900s"`
	F051                float64 `parquet:"taker_imbalance_1s"`
	F052                float64 `parquet:"taker_imbalance_5s"`
	F053                float64 `parquet:"taker_imbalance_10s"`
	F054                float64 `parquet:"taker_imbalance_30s"`
	F055                float64 `parquet:"taker_imbalance_60s"`
	F056                float64 `parquet:"taker_imbalance_300s"`
	F057                float64 `parquet:"taker_imbalance_900s"`
	F058                float64 `parquet:"vwap_deviation_30s"`
	F059                float64 `parquet:"vwap_deviation_60s"`
	F060                float64 `parquet:"vwap_deviation_300s"`
	F061                float64 `parquet:"vwap_deviation_900s"`
	F062                float64 `parquet:"trade_intensity_5s"`
	F063                float64 `parquet:"trade_intensity_30s"`
	F064                float64 `parquet:"trade_intensity_60s"`
	F065                float64 `parquet:"trade_intensity_300s"`
	F066                float64 `parquet:"no_trade_ratio_30s"`
	F067                float64 `parquet:"no_trade_ratio_60s"`
	F068                float64 `parquet:"no_trade_ratio_300s"`
	F069                float64 `parquet:"no_trade_ratio_900s"`
	F070                float64 `parquet:"momentum_accel_5s"`
	F071                float64 `parquet:"momentum_accel_30s"`
	F072                float64 `parquet:"momentum_accel_60s"`
	F073                float64 `parquet:"imbalance_change_5s"`
	F074                float64 `parquet:"imbalance_change_30s"`
	F075                float64 `parquet:"imbalance_change_60s"`
	F076                float64 `parquet:"volume_ratio_5s_60s"`
	F077                float64 `parquet:"trade_intensity_ratio_5s_60s"`
	F078                float64 `parquet:"volume_ratio_30s_300s"`
	F079                float64 `parquet:"trade_intensity_ratio_30s_300s"`
	F080                float64 `parquet:"spot_return_5s"`
	F081                float64 `parquet:"spot_return_30s"`
	F082                float64 `parquet:"spot_return_60s"`
	F083                float64 `parquet:"spot_return_300s"`
	F084                float64 `parquet:"spot_taker_imbalance_5s"`
	F085                float64 `parquet:"spot_taker_imbalance_30s"`
	F086                float64 `parquet:"spot_taker_imbalance_60s"`
	F087                float64 `parquet:"spot_volume_30s"`
	F088                float64 `parquet:"spot_volume_300s"`
	F089                float64 `parquet:"spot_trade_intensity_30s"`
	F090                float64 `parquet:"spot_perp_return_gap_5s"`
	F091                float64 `parquet:"spot_perp_return_gap_30s"`
	F092                float64 `parquet:"spot_perp_return_gap_60s"`
	F093                float64 `parquet:"spot_perp_return_gap_300s"`
	F094                float64 `parquet:"spot_perp_taker_imbalance_gap_5s"`
	F095                float64 `parquet:"spot_perp_taker_imbalance_gap_30s"`
	F096                float64 `parquet:"spot_perp_taker_imbalance_gap_60s"`
	F097                float64 `parquet:"spot_perp_volume_ratio_30s"`
	F098                float64 `parquet:"spot_perp_volume_ratio_300s"`
	F099                float64 `parquet:"spot_perp_spread_bps"`
	F100                float64 `parquet:"oi_change_pct_5m"`
	F101                float64 `parquet:"oi_change_pct_15m"`
	F102                float64 `parquet:"oi_change_pct_60m"`
	F103                float64 `parquet:"oi_value_change_pct_5m"`
	F104                float64 `parquet:"oi_value_change_pct_15m"`
	F105                float64 `parquet:"oi_value_change_pct_60m"`
	F106                float64 `parquet:"price_return_x_oi_change_5m"`
	F107                float64 `parquet:"price_return_x_oi_change_15m"`
	F108                float64 `parquet:"top_trader_account_ls_ratio"`
	F109                float64 `parquet:"top_trader_account_ls_ratio_change_5m"`
	F110                float64 `parquet:"top_trader_position_ls_ratio"`
	F111                float64 `parquet:"top_trader_position_ls_ratio_change_5m"`
	F112                float64 `parquet:"global_ls_ratio"`
	F113                float64 `parquet:"global_ls_ratio_change_5m"`
	F114                float64 `parquet:"taker_long_short_volume_ratio"`
	F115                float64 `parquet:"taker_long_short_volume_ratio_change_5m"`
	F116                float64 `parquet:"top_vs_global_positioning_gap"`
	F117                float64 `parquet:"mark_index_spread_bps"`
	F118                float64 `parquet:"contract_index_spread_bps"`
	F119                float64 `parquet:"premium_close"`
	F120                float64 `parquet:"premium_change_5m"`
	F121                float64 `parquet:"premium_change_15m"`
	F122                float64 `parquet:"mark_index_spread_change_5m"`
	F123                float64 `parquet:"last_funding_rate"`
	F124                float64 `parquet:"funding_age_ms"`
	F125                float64 `parquet:"funding_rate_change"`
	F126                float64 `parquet:"metrics_age_ms"`
	F127                float64 `parquet:"kline_age_ms"`
}

func RowFromSnapshot(s Snapshot) FeatureRowV2 {
	return FeatureRowV2{
		DecisionTimestampMs: s.DecisionTimestampMs,
		F000:                s.Values[0],
		F001:                s.Values[1],
		F002:                s.Values[2],
		F003:                s.Values[3],
		F004:                s.Values[4],
		F005:                s.Values[5],
		F006:                s.Values[6],
		F007:                s.Values[7],
		F008:                s.Values[8],
		F009:                s.Values[9],
		F010:                s.Values[10],
		F011:                s.Values[11],
		F012:                s.Values[12],
		F013:                s.Values[13],
		F014:                s.Values[14],
		F015:                s.Values[15],
		F016:                s.Values[16],
		F017:                s.Values[17],
		F018:                s.Values[18],
		F019:                s.Values[19],
		F020:                s.Values[20],
		F021:                s.Values[21],
		F022:                s.Values[22],
		F023:                s.Values[23],
		F024:                s.Values[24],
		F025:                s.Values[25],
		F026:                s.Values[26],
		F027:                s.Values[27],
		F028:                s.Values[28],
		F029:                s.Values[29],
		F030:                s.Values[30],
		F031:                s.Values[31],
		F032:                s.Values[32],
		F033:                s.Values[33],
		F034:                s.Values[34],
		F035:                s.Values[35],
		F036:                s.Values[36],
		F037:                s.Values[37],
		F038:                s.Values[38],
		F039:                s.Values[39],
		F040:                s.Values[40],
		F041:                s.Values[41],
		F042:                s.Values[42],
		F043:                s.Values[43],
		F044:                s.Values[44],
		F045:                s.Values[45],
		F046:                s.Values[46],
		F047:                s.Values[47],
		F048:                s.Values[48],
		F049:                s.Values[49],
		F050:                s.Values[50],
		F051:                s.Values[51],
		F052:                s.Values[52],
		F053:                s.Values[53],
		F054:                s.Values[54],
		F055:                s.Values[55],
		F056:                s.Values[56],
		F057:                s.Values[57],
		F058:                s.Values[58],
		F059:                s.Values[59],
		F060:                s.Values[60],
		F061:                s.Values[61],
		F062:                s.Values[62],
		F063:                s.Values[63],
		F064:                s.Values[64],
		F065:                s.Values[65],
		F066:                s.Values[66],
		F067:                s.Values[67],
		F068:                s.Values[68],
		F069:                s.Values[69],
		F070:                s.Values[70],
		F071:                s.Values[71],
		F072:                s.Values[72],
		F073:                s.Values[73],
		F074:                s.Values[74],
		F075:                s.Values[75],
		F076:                s.Values[76],
		F077:                s.Values[77],
		F078:                s.Values[78],
		F079:                s.Values[79],
		F080:                s.Values[80],
		F081:                s.Values[81],
		F082:                s.Values[82],
		F083:                s.Values[83],
		F084:                s.Values[84],
		F085:                s.Values[85],
		F086:                s.Values[86],
		F087:                s.Values[87],
		F088:                s.Values[88],
		F089:                s.Values[89],
		F090:                s.Values[90],
		F091:                s.Values[91],
		F092:                s.Values[92],
		F093:                s.Values[93],
		F094:                s.Values[94],
		F095:                s.Values[95],
		F096:                s.Values[96],
		F097:                s.Values[97],
		F098:                s.Values[98],
		F099:                s.Values[99],
		F100:                s.Values[100],
		F101:                s.Values[101],
		F102:                s.Values[102],
		F103:                s.Values[103],
		F104:                s.Values[104],
		F105:                s.Values[105],
		F106:                s.Values[106],
		F107:                s.Values[107],
		F108:                s.Values[108],
		F109:                s.Values[109],
		F110:                s.Values[110],
		F111:                s.Values[111],
		F112:                s.Values[112],
		F113:                s.Values[113],
		F114:                s.Values[114],
		F115:                s.Values[115],
		F116:                s.Values[116],
		F117:                s.Values[117],
		F118:                s.Values[118],
		F119:                s.Values[119],
		F120:                s.Values[120],
		F121:                s.Values[121],
		F122:                s.Values[122],
		F123:                s.Values[123],
		F124:                s.Values[124],
		F125:                s.Values[125],
		F126:                s.Values[126],
		F127:                s.Values[127],
	}
}
func (r FeatureRowV2) FeatureValues() [ModelFeatureCountV2]float64 {
	return [ModelFeatureCountV2]float64{
		r.F000,
		r.F001,
		r.F002,
		r.F003,
		r.F004,
		r.F005,
		r.F006,
		r.F007,
		r.F008,
		r.F009,
		r.F010,
		r.F011,
		r.F012,
		r.F013,
		r.F014,
		r.F015,
		r.F016,
		r.F017,
		r.F018,
		r.F019,
		r.F020,
		r.F021,
		r.F022,
		r.F023,
		r.F024,
		r.F025,
		r.F026,
		r.F027,
		r.F028,
		r.F029,
		r.F030,
		r.F031,
		r.F032,
		r.F033,
		r.F034,
		r.F035,
		r.F036,
		r.F037,
		r.F038,
		r.F039,
		r.F040,
		r.F041,
		r.F042,
		r.F043,
		r.F044,
		r.F045,
		r.F046,
		r.F047,
		r.F048,
		r.F049,
		r.F050,
		r.F051,
		r.F052,
		r.F053,
		r.F054,
		r.F055,
		r.F056,
		r.F057,
		r.F058,
		r.F059,
		r.F060,
		r.F061,
		r.F062,
		r.F063,
		r.F064,
		r.F065,
		r.F066,
		r.F067,
		r.F068,
		r.F069,
		r.F070,
		r.F071,
		r.F072,
		r.F073,
		r.F074,
		r.F075,
		r.F076,
		r.F077,
		r.F078,
		r.F079,
		r.F080,
		r.F081,
		r.F082,
		r.F083,
		r.F084,
		r.F085,
		r.F086,
		r.F087,
		r.F088,
		r.F089,
		r.F090,
		r.F091,
		r.F092,
		r.F093,
		r.F094,
		r.F095,
		r.F096,
		r.F097,
		r.F098,
		r.F099,
		r.F100,
		r.F101,
		r.F102,
		r.F103,
		r.F104,
		r.F105,
		r.F106,
		r.F107,
		r.F108,
		r.F109,
		r.F110,
		r.F111,
		r.F112,
		r.F113,
		r.F114,
		r.F115,
		r.F116,
		r.F117,
		r.F118,
		r.F119,
		r.F120,
		r.F121,
		r.F122,
		r.F123,
		r.F124,
		r.F125,
		r.F126,
		r.F127,
	}
}
