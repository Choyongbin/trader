package mainbarrier

const BarrierVersion = 1
const MaxHorizonSeconds = 14400

var BarrierGridBps = [...]int{5, 10, 15, 20, 25, 30, 40, 50, 75, 100, 150, 200, 300, 500, 750, 1000}

type BarrierHit struct {
	TimestampMs int64
	AggTradeID  int64
	Price       float64
}

type BarrierOutcomeV1 struct {
	DecisionTimestampMs    int64   `parquet:"decision_timestamp_ms" json:"decision_timestamp_ms"`
	EntryDelayMs           int64   `parquet:"entry_delay_ms" json:"entry_delay_ms"`
	EntryTradeTimestampMs  int64   `parquet:"entry_trade_timestamp_ms" json:"entry_trade_timestamp_ms"`
	EntryAggTradeID        int64   `parquet:"entry_agg_trade_id" json:"entry_agg_trade_id"`
	EntryReferencePrice    float64 `parquet:"entry_reference_price" json:"entry_reference_price"`
	EntryWaitMs            int64   `parquet:"entry_wait_ms" json:"entry_wait_ms"`
	EntryAvailable         bool    `parquet:"entry_available" json:"entry_available"`
	Up5BpsTimestampMs      int64   `parquet:"up_5bps_timestamp_ms" json:"up_5bps_timestamp_ms"`
	Up5BpsAggTradeID       int64   `parquet:"up_5bps_agg_trade_id" json:"up_5bps_agg_trade_id"`
	Up5BpsPrice            float64 `parquet:"up_5bps_price" json:"up_5bps_price"`
	Up10BpsTimestampMs     int64   `parquet:"up_10bps_timestamp_ms" json:"up_10bps_timestamp_ms"`
	Up10BpsAggTradeID      int64   `parquet:"up_10bps_agg_trade_id" json:"up_10bps_agg_trade_id"`
	Up10BpsPrice           float64 `parquet:"up_10bps_price" json:"up_10bps_price"`
	Up15BpsTimestampMs     int64   `parquet:"up_15bps_timestamp_ms" json:"up_15bps_timestamp_ms"`
	Up15BpsAggTradeID      int64   `parquet:"up_15bps_agg_trade_id" json:"up_15bps_agg_trade_id"`
	Up15BpsPrice           float64 `parquet:"up_15bps_price" json:"up_15bps_price"`
	Up20BpsTimestampMs     int64   `parquet:"up_20bps_timestamp_ms" json:"up_20bps_timestamp_ms"`
	Up20BpsAggTradeID      int64   `parquet:"up_20bps_agg_trade_id" json:"up_20bps_agg_trade_id"`
	Up20BpsPrice           float64 `parquet:"up_20bps_price" json:"up_20bps_price"`
	Up25BpsTimestampMs     int64   `parquet:"up_25bps_timestamp_ms" json:"up_25bps_timestamp_ms"`
	Up25BpsAggTradeID      int64   `parquet:"up_25bps_agg_trade_id" json:"up_25bps_agg_trade_id"`
	Up25BpsPrice           float64 `parquet:"up_25bps_price" json:"up_25bps_price"`
	Up30BpsTimestampMs     int64   `parquet:"up_30bps_timestamp_ms" json:"up_30bps_timestamp_ms"`
	Up30BpsAggTradeID      int64   `parquet:"up_30bps_agg_trade_id" json:"up_30bps_agg_trade_id"`
	Up30BpsPrice           float64 `parquet:"up_30bps_price" json:"up_30bps_price"`
	Up40BpsTimestampMs     int64   `parquet:"up_40bps_timestamp_ms" json:"up_40bps_timestamp_ms"`
	Up40BpsAggTradeID      int64   `parquet:"up_40bps_agg_trade_id" json:"up_40bps_agg_trade_id"`
	Up40BpsPrice           float64 `parquet:"up_40bps_price" json:"up_40bps_price"`
	Up50BpsTimestampMs     int64   `parquet:"up_50bps_timestamp_ms" json:"up_50bps_timestamp_ms"`
	Up50BpsAggTradeID      int64   `parquet:"up_50bps_agg_trade_id" json:"up_50bps_agg_trade_id"`
	Up50BpsPrice           float64 `parquet:"up_50bps_price" json:"up_50bps_price"`
	Up75BpsTimestampMs     int64   `parquet:"up_75bps_timestamp_ms" json:"up_75bps_timestamp_ms"`
	Up75BpsAggTradeID      int64   `parquet:"up_75bps_agg_trade_id" json:"up_75bps_agg_trade_id"`
	Up75BpsPrice           float64 `parquet:"up_75bps_price" json:"up_75bps_price"`
	Up100BpsTimestampMs    int64   `parquet:"up_100bps_timestamp_ms" json:"up_100bps_timestamp_ms"`
	Up100BpsAggTradeID     int64   `parquet:"up_100bps_agg_trade_id" json:"up_100bps_agg_trade_id"`
	Up100BpsPrice          float64 `parquet:"up_100bps_price" json:"up_100bps_price"`
	Up150BpsTimestampMs    int64   `parquet:"up_150bps_timestamp_ms" json:"up_150bps_timestamp_ms"`
	Up150BpsAggTradeID     int64   `parquet:"up_150bps_agg_trade_id" json:"up_150bps_agg_trade_id"`
	Up150BpsPrice          float64 `parquet:"up_150bps_price" json:"up_150bps_price"`
	Up200BpsTimestampMs    int64   `parquet:"up_200bps_timestamp_ms" json:"up_200bps_timestamp_ms"`
	Up200BpsAggTradeID     int64   `parquet:"up_200bps_agg_trade_id" json:"up_200bps_agg_trade_id"`
	Up200BpsPrice          float64 `parquet:"up_200bps_price" json:"up_200bps_price"`
	Up300BpsTimestampMs    int64   `parquet:"up_300bps_timestamp_ms" json:"up_300bps_timestamp_ms"`
	Up300BpsAggTradeID     int64   `parquet:"up_300bps_agg_trade_id" json:"up_300bps_agg_trade_id"`
	Up300BpsPrice          float64 `parquet:"up_300bps_price" json:"up_300bps_price"`
	Up500BpsTimestampMs    int64   `parquet:"up_500bps_timestamp_ms" json:"up_500bps_timestamp_ms"`
	Up500BpsAggTradeID     int64   `parquet:"up_500bps_agg_trade_id" json:"up_500bps_agg_trade_id"`
	Up500BpsPrice          float64 `parquet:"up_500bps_price" json:"up_500bps_price"`
	Up750BpsTimestampMs    int64   `parquet:"up_750bps_timestamp_ms" json:"up_750bps_timestamp_ms"`
	Up750BpsAggTradeID     int64   `parquet:"up_750bps_agg_trade_id" json:"up_750bps_agg_trade_id"`
	Up750BpsPrice          float64 `parquet:"up_750bps_price" json:"up_750bps_price"`
	Up1000BpsTimestampMs   int64   `parquet:"up_1000bps_timestamp_ms" json:"up_1000bps_timestamp_ms"`
	Up1000BpsAggTradeID    int64   `parquet:"up_1000bps_agg_trade_id" json:"up_1000bps_agg_trade_id"`
	Up1000BpsPrice         float64 `parquet:"up_1000bps_price" json:"up_1000bps_price"`
	Down5BpsTimestampMs    int64   `parquet:"down_5bps_timestamp_ms" json:"down_5bps_timestamp_ms"`
	Down5BpsAggTradeID     int64   `parquet:"down_5bps_agg_trade_id" json:"down_5bps_agg_trade_id"`
	Down5BpsPrice          float64 `parquet:"down_5bps_price" json:"down_5bps_price"`
	Down10BpsTimestampMs   int64   `parquet:"down_10bps_timestamp_ms" json:"down_10bps_timestamp_ms"`
	Down10BpsAggTradeID    int64   `parquet:"down_10bps_agg_trade_id" json:"down_10bps_agg_trade_id"`
	Down10BpsPrice         float64 `parquet:"down_10bps_price" json:"down_10bps_price"`
	Down15BpsTimestampMs   int64   `parquet:"down_15bps_timestamp_ms" json:"down_15bps_timestamp_ms"`
	Down15BpsAggTradeID    int64   `parquet:"down_15bps_agg_trade_id" json:"down_15bps_agg_trade_id"`
	Down15BpsPrice         float64 `parquet:"down_15bps_price" json:"down_15bps_price"`
	Down20BpsTimestampMs   int64   `parquet:"down_20bps_timestamp_ms" json:"down_20bps_timestamp_ms"`
	Down20BpsAggTradeID    int64   `parquet:"down_20bps_agg_trade_id" json:"down_20bps_agg_trade_id"`
	Down20BpsPrice         float64 `parquet:"down_20bps_price" json:"down_20bps_price"`
	Down25BpsTimestampMs   int64   `parquet:"down_25bps_timestamp_ms" json:"down_25bps_timestamp_ms"`
	Down25BpsAggTradeID    int64   `parquet:"down_25bps_agg_trade_id" json:"down_25bps_agg_trade_id"`
	Down25BpsPrice         float64 `parquet:"down_25bps_price" json:"down_25bps_price"`
	Down30BpsTimestampMs   int64   `parquet:"down_30bps_timestamp_ms" json:"down_30bps_timestamp_ms"`
	Down30BpsAggTradeID    int64   `parquet:"down_30bps_agg_trade_id" json:"down_30bps_agg_trade_id"`
	Down30BpsPrice         float64 `parquet:"down_30bps_price" json:"down_30bps_price"`
	Down40BpsTimestampMs   int64   `parquet:"down_40bps_timestamp_ms" json:"down_40bps_timestamp_ms"`
	Down40BpsAggTradeID    int64   `parquet:"down_40bps_agg_trade_id" json:"down_40bps_agg_trade_id"`
	Down40BpsPrice         float64 `parquet:"down_40bps_price" json:"down_40bps_price"`
	Down50BpsTimestampMs   int64   `parquet:"down_50bps_timestamp_ms" json:"down_50bps_timestamp_ms"`
	Down50BpsAggTradeID    int64   `parquet:"down_50bps_agg_trade_id" json:"down_50bps_agg_trade_id"`
	Down50BpsPrice         float64 `parquet:"down_50bps_price" json:"down_50bps_price"`
	Down75BpsTimestampMs   int64   `parquet:"down_75bps_timestamp_ms" json:"down_75bps_timestamp_ms"`
	Down75BpsAggTradeID    int64   `parquet:"down_75bps_agg_trade_id" json:"down_75bps_agg_trade_id"`
	Down75BpsPrice         float64 `parquet:"down_75bps_price" json:"down_75bps_price"`
	Down100BpsTimestampMs  int64   `parquet:"down_100bps_timestamp_ms" json:"down_100bps_timestamp_ms"`
	Down100BpsAggTradeID   int64   `parquet:"down_100bps_agg_trade_id" json:"down_100bps_agg_trade_id"`
	Down100BpsPrice        float64 `parquet:"down_100bps_price" json:"down_100bps_price"`
	Down150BpsTimestampMs  int64   `parquet:"down_150bps_timestamp_ms" json:"down_150bps_timestamp_ms"`
	Down150BpsAggTradeID   int64   `parquet:"down_150bps_agg_trade_id" json:"down_150bps_agg_trade_id"`
	Down150BpsPrice        float64 `parquet:"down_150bps_price" json:"down_150bps_price"`
	Down200BpsTimestampMs  int64   `parquet:"down_200bps_timestamp_ms" json:"down_200bps_timestamp_ms"`
	Down200BpsAggTradeID   int64   `parquet:"down_200bps_agg_trade_id" json:"down_200bps_agg_trade_id"`
	Down200BpsPrice        float64 `parquet:"down_200bps_price" json:"down_200bps_price"`
	Down300BpsTimestampMs  int64   `parquet:"down_300bps_timestamp_ms" json:"down_300bps_timestamp_ms"`
	Down300BpsAggTradeID   int64   `parquet:"down_300bps_agg_trade_id" json:"down_300bps_agg_trade_id"`
	Down300BpsPrice        float64 `parquet:"down_300bps_price" json:"down_300bps_price"`
	Down500BpsTimestampMs  int64   `parquet:"down_500bps_timestamp_ms" json:"down_500bps_timestamp_ms"`
	Down500BpsAggTradeID   int64   `parquet:"down_500bps_agg_trade_id" json:"down_500bps_agg_trade_id"`
	Down500BpsPrice        float64 `parquet:"down_500bps_price" json:"down_500bps_price"`
	Down750BpsTimestampMs  int64   `parquet:"down_750bps_timestamp_ms" json:"down_750bps_timestamp_ms"`
	Down750BpsAggTradeID   int64   `parquet:"down_750bps_agg_trade_id" json:"down_750bps_agg_trade_id"`
	Down750BpsPrice        float64 `parquet:"down_750bps_price" json:"down_750bps_price"`
	Down1000BpsTimestampMs int64   `parquet:"down_1000bps_timestamp_ms" json:"down_1000bps_timestamp_ms"`
	Down1000BpsAggTradeID  int64   `parquet:"down_1000bps_agg_trade_id" json:"down_1000bps_agg_trade_id"`
	Down1000BpsPrice       float64 `parquet:"down_1000bps_price" json:"down_1000bps_price"`
}

func (o *BarrierOutcomeV1) setHit(up bool, index int, h BarrierHit) {
	switch index {
	case 0:
		if up {
			o.Up5BpsTimestampMs = h.TimestampMs
			o.Up5BpsAggTradeID = h.AggTradeID
			o.Up5BpsPrice = h.Price
		} else {
			o.Down5BpsTimestampMs = h.TimestampMs
			o.Down5BpsAggTradeID = h.AggTradeID
			o.Down5BpsPrice = h.Price
		}
	case 1:
		if up {
			o.Up10BpsTimestampMs = h.TimestampMs
			o.Up10BpsAggTradeID = h.AggTradeID
			o.Up10BpsPrice = h.Price
		} else {
			o.Down10BpsTimestampMs = h.TimestampMs
			o.Down10BpsAggTradeID = h.AggTradeID
			o.Down10BpsPrice = h.Price
		}
	case 2:
		if up {
			o.Up15BpsTimestampMs = h.TimestampMs
			o.Up15BpsAggTradeID = h.AggTradeID
			o.Up15BpsPrice = h.Price
		} else {
			o.Down15BpsTimestampMs = h.TimestampMs
			o.Down15BpsAggTradeID = h.AggTradeID
			o.Down15BpsPrice = h.Price
		}
	case 3:
		if up {
			o.Up20BpsTimestampMs = h.TimestampMs
			o.Up20BpsAggTradeID = h.AggTradeID
			o.Up20BpsPrice = h.Price
		} else {
			o.Down20BpsTimestampMs = h.TimestampMs
			o.Down20BpsAggTradeID = h.AggTradeID
			o.Down20BpsPrice = h.Price
		}
	case 4:
		if up {
			o.Up25BpsTimestampMs = h.TimestampMs
			o.Up25BpsAggTradeID = h.AggTradeID
			o.Up25BpsPrice = h.Price
		} else {
			o.Down25BpsTimestampMs = h.TimestampMs
			o.Down25BpsAggTradeID = h.AggTradeID
			o.Down25BpsPrice = h.Price
		}
	case 5:
		if up {
			o.Up30BpsTimestampMs = h.TimestampMs
			o.Up30BpsAggTradeID = h.AggTradeID
			o.Up30BpsPrice = h.Price
		} else {
			o.Down30BpsTimestampMs = h.TimestampMs
			o.Down30BpsAggTradeID = h.AggTradeID
			o.Down30BpsPrice = h.Price
		}
	case 6:
		if up {
			o.Up40BpsTimestampMs = h.TimestampMs
			o.Up40BpsAggTradeID = h.AggTradeID
			o.Up40BpsPrice = h.Price
		} else {
			o.Down40BpsTimestampMs = h.TimestampMs
			o.Down40BpsAggTradeID = h.AggTradeID
			o.Down40BpsPrice = h.Price
		}
	case 7:
		if up {
			o.Up50BpsTimestampMs = h.TimestampMs
			o.Up50BpsAggTradeID = h.AggTradeID
			o.Up50BpsPrice = h.Price
		} else {
			o.Down50BpsTimestampMs = h.TimestampMs
			o.Down50BpsAggTradeID = h.AggTradeID
			o.Down50BpsPrice = h.Price
		}
	case 8:
		if up {
			o.Up75BpsTimestampMs = h.TimestampMs
			o.Up75BpsAggTradeID = h.AggTradeID
			o.Up75BpsPrice = h.Price
		} else {
			o.Down75BpsTimestampMs = h.TimestampMs
			o.Down75BpsAggTradeID = h.AggTradeID
			o.Down75BpsPrice = h.Price
		}
	case 9:
		if up {
			o.Up100BpsTimestampMs = h.TimestampMs
			o.Up100BpsAggTradeID = h.AggTradeID
			o.Up100BpsPrice = h.Price
		} else {
			o.Down100BpsTimestampMs = h.TimestampMs
			o.Down100BpsAggTradeID = h.AggTradeID
			o.Down100BpsPrice = h.Price
		}
	case 10:
		if up {
			o.Up150BpsTimestampMs = h.TimestampMs
			o.Up150BpsAggTradeID = h.AggTradeID
			o.Up150BpsPrice = h.Price
		} else {
			o.Down150BpsTimestampMs = h.TimestampMs
			o.Down150BpsAggTradeID = h.AggTradeID
			o.Down150BpsPrice = h.Price
		}
	case 11:
		if up {
			o.Up200BpsTimestampMs = h.TimestampMs
			o.Up200BpsAggTradeID = h.AggTradeID
			o.Up200BpsPrice = h.Price
		} else {
			o.Down200BpsTimestampMs = h.TimestampMs
			o.Down200BpsAggTradeID = h.AggTradeID
			o.Down200BpsPrice = h.Price
		}
	case 12:
		if up {
			o.Up300BpsTimestampMs = h.TimestampMs
			o.Up300BpsAggTradeID = h.AggTradeID
			o.Up300BpsPrice = h.Price
		} else {
			o.Down300BpsTimestampMs = h.TimestampMs
			o.Down300BpsAggTradeID = h.AggTradeID
			o.Down300BpsPrice = h.Price
		}
	case 13:
		if up {
			o.Up500BpsTimestampMs = h.TimestampMs
			o.Up500BpsAggTradeID = h.AggTradeID
			o.Up500BpsPrice = h.Price
		} else {
			o.Down500BpsTimestampMs = h.TimestampMs
			o.Down500BpsAggTradeID = h.AggTradeID
			o.Down500BpsPrice = h.Price
		}
	case 14:
		if up {
			o.Up750BpsTimestampMs = h.TimestampMs
			o.Up750BpsAggTradeID = h.AggTradeID
			o.Up750BpsPrice = h.Price
		} else {
			o.Down750BpsTimestampMs = h.TimestampMs
			o.Down750BpsAggTradeID = h.AggTradeID
			o.Down750BpsPrice = h.Price
		}
	case 15:
		if up {
			o.Up1000BpsTimestampMs = h.TimestampMs
			o.Up1000BpsAggTradeID = h.AggTradeID
			o.Up1000BpsPrice = h.Price
		} else {
			o.Down1000BpsTimestampMs = h.TimestampMs
			o.Down1000BpsAggTradeID = h.AggTradeID
			o.Down1000BpsPrice = h.Price
		}
	}
}

func (o BarrierOutcomeV1) Hit(up bool, bps int) (BarrierHit, bool) {
	switch bps {
	case 5:
		if up {
			return BarrierHit{o.Up5BpsTimestampMs, o.Up5BpsAggTradeID, o.Up5BpsPrice}, true
		}
		return BarrierHit{o.Down5BpsTimestampMs, o.Down5BpsAggTradeID, o.Down5BpsPrice}, true
	case 10:
		if up {
			return BarrierHit{o.Up10BpsTimestampMs, o.Up10BpsAggTradeID, o.Up10BpsPrice}, true
		}
		return BarrierHit{o.Down10BpsTimestampMs, o.Down10BpsAggTradeID, o.Down10BpsPrice}, true
	case 15:
		if up {
			return BarrierHit{o.Up15BpsTimestampMs, o.Up15BpsAggTradeID, o.Up15BpsPrice}, true
		}
		return BarrierHit{o.Down15BpsTimestampMs, o.Down15BpsAggTradeID, o.Down15BpsPrice}, true
	case 20:
		if up {
			return BarrierHit{o.Up20BpsTimestampMs, o.Up20BpsAggTradeID, o.Up20BpsPrice}, true
		}
		return BarrierHit{o.Down20BpsTimestampMs, o.Down20BpsAggTradeID, o.Down20BpsPrice}, true
	case 25:
		if up {
			return BarrierHit{o.Up25BpsTimestampMs, o.Up25BpsAggTradeID, o.Up25BpsPrice}, true
		}
		return BarrierHit{o.Down25BpsTimestampMs, o.Down25BpsAggTradeID, o.Down25BpsPrice}, true
	case 30:
		if up {
			return BarrierHit{o.Up30BpsTimestampMs, o.Up30BpsAggTradeID, o.Up30BpsPrice}, true
		}
		return BarrierHit{o.Down30BpsTimestampMs, o.Down30BpsAggTradeID, o.Down30BpsPrice}, true
	case 40:
		if up {
			return BarrierHit{o.Up40BpsTimestampMs, o.Up40BpsAggTradeID, o.Up40BpsPrice}, true
		}
		return BarrierHit{o.Down40BpsTimestampMs, o.Down40BpsAggTradeID, o.Down40BpsPrice}, true
	case 50:
		if up {
			return BarrierHit{o.Up50BpsTimestampMs, o.Up50BpsAggTradeID, o.Up50BpsPrice}, true
		}
		return BarrierHit{o.Down50BpsTimestampMs, o.Down50BpsAggTradeID, o.Down50BpsPrice}, true
	case 75:
		if up {
			return BarrierHit{o.Up75BpsTimestampMs, o.Up75BpsAggTradeID, o.Up75BpsPrice}, true
		}
		return BarrierHit{o.Down75BpsTimestampMs, o.Down75BpsAggTradeID, o.Down75BpsPrice}, true
	case 100:
		if up {
			return BarrierHit{o.Up100BpsTimestampMs, o.Up100BpsAggTradeID, o.Up100BpsPrice}, true
		}
		return BarrierHit{o.Down100BpsTimestampMs, o.Down100BpsAggTradeID, o.Down100BpsPrice}, true
	case 150:
		if up {
			return BarrierHit{o.Up150BpsTimestampMs, o.Up150BpsAggTradeID, o.Up150BpsPrice}, true
		}
		return BarrierHit{o.Down150BpsTimestampMs, o.Down150BpsAggTradeID, o.Down150BpsPrice}, true
	case 200:
		if up {
			return BarrierHit{o.Up200BpsTimestampMs, o.Up200BpsAggTradeID, o.Up200BpsPrice}, true
		}
		return BarrierHit{o.Down200BpsTimestampMs, o.Down200BpsAggTradeID, o.Down200BpsPrice}, true
	case 300:
		if up {
			return BarrierHit{o.Up300BpsTimestampMs, o.Up300BpsAggTradeID, o.Up300BpsPrice}, true
		}
		return BarrierHit{o.Down300BpsTimestampMs, o.Down300BpsAggTradeID, o.Down300BpsPrice}, true
	case 500:
		if up {
			return BarrierHit{o.Up500BpsTimestampMs, o.Up500BpsAggTradeID, o.Up500BpsPrice}, true
		}
		return BarrierHit{o.Down500BpsTimestampMs, o.Down500BpsAggTradeID, o.Down500BpsPrice}, true
	case 750:
		if up {
			return BarrierHit{o.Up750BpsTimestampMs, o.Up750BpsAggTradeID, o.Up750BpsPrice}, true
		}
		return BarrierHit{o.Down750BpsTimestampMs, o.Down750BpsAggTradeID, o.Down750BpsPrice}, true
	case 1000:
		if up {
			return BarrierHit{o.Up1000BpsTimestampMs, o.Up1000BpsAggTradeID, o.Up1000BpsPrice}, true
		}
		return BarrierHit{o.Down1000BpsTimestampMs, o.Down1000BpsAggTradeID, o.Down1000BpsPrice}, true
	default:
		return BarrierHit{}, false
	}
}
