package calibration

import (
	"fmt"
	"math"
	"sort"

	logistic "binance_trader/internal/model/logistic"
)

func plattLoss(raw []float64, targets []bool, a, b float64) float64 {
	sum := 0.0
	for i, p := range raw {
		z := a*rawLogit(p) + b
		y := 0.0
		if targets[i] {
			y = 1
		}
		sum += logistic.Softplus(z) - y*z
	}
	return sum / float64(len(raw))
}
func FitPlatt(raw []float64, targets []bool) (PlattModel, error) {
	if len(raw) == 0 || len(raw) != len(targets) {
		return PlattModel{}, fmt.Errorf("invalid Platt input")
	}
	a, b := 1.0, 0.0
	ridge := 1e-12
	var norm float64
	for iteration := 1; iteration <= 50; iteration++ {
		var ga, gb, haa, hab, hbb float64
		for i, p := range raw {
			x := rawLogit(p)
			q := logistic.Sigmoid(a*x + b)
			y := 0.0
			if targets[i] {
				y = 1
			}
			d := q - y
			v := q * (1 - q)
			ga += d * x
			gb += d
			haa += v * x * x
			hab += v * x
			hbb += v
		}
		n := float64(len(raw))
		ga /= n
		gb /= n
		haa = haa/n + ridge
		hab /= n
		hbb = hbb/n + ridge
		norm = math.Hypot(ga, gb)
		if norm < 1e-9 {
			return PlattModel{a, b, iteration, true, norm, ridge}, nil
		}
		det := haa*hbb - hab*hab
		if det <= 0 {
			return PlattModel{}, fmt.Errorf("singular Platt Hessian")
		}
		da := (hbb*ga - hab*gb) / det
		db := (-hab*ga + haa*gb) / det
		old := plattLoss(raw, targets, a, b)
		step := 1.0
		for step > 1e-8 && plattLoss(raw, targets, a-step*da, b-step*db) > old {
			step /= 2
		}
		a -= step * da
		b -= step * db
		if step <= 1e-8 {
			return PlattModel{a, b, iteration, false, norm, ridge}, fmt.Errorf("Platt line search failed")
		}
	}
	return PlattModel{a, b, 50, false, norm, ridge}, fmt.Errorf("Platt did not converge")
}

type isoPoint struct {
	score    float64
	positive bool
}
type isoBlock struct {
	max             float64
	count, positive int64
}

func FitIsotonic(raw []float64, targets []bool) (IsotonicModel, error) {
	if len(raw) == 0 || len(raw) != len(targets) {
		return IsotonicModel{}, fmt.Errorf("invalid isotonic input")
	}
	points := make([]isoPoint, len(raw))
	for i := range raw {
		points[i] = isoPoint{raw[i], targets[i]}
	}
	sort.Slice(points, func(i, j int) bool { return points[i].score < points[j].score })
	groups := make([]isoBlock, 0)
	for i := 0; i < len(points); {
		j := i
		var positive int64
		for j < len(points) && points[j].score == points[i].score {
			if points[j].positive {
				positive++
			}
			j++
		}
		groups = append(groups, isoBlock{points[i].score, int64(j - i), positive})
		i = j
	}
	blocks := make([]isoBlock, 0, len(groups))
	for _, g := range groups {
		blocks = append(blocks, g)
		for len(blocks) >= 2 {
			n := len(blocks)
			left, right := blocks[n-2], blocks[n-1]
			if float64(left.positive)/float64(left.count) <= float64(right.positive)/float64(right.count) {
				break
			}
			blocks[n-2] = isoBlock{right.max, left.count + right.count, left.positive + right.positive}
			blocks = blocks[:n-1]
		}
	}
	m := IsotonicModel{TieGroups: len(groups), Blocks: len(blocks), OutOfRange: "clamp_to_nearest_edge_mapping"}
	for _, b := range blocks {
		m.Thresholds = append(m.Thresholds, b.max)
		m.Values = append(m.Values, float64(b.positive)/float64(b.count))
	}
	return m, nil
}
