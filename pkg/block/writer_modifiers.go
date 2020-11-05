package block

import (
	"math"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/tsdb/tombstones"
)

type Modifier interface {
	Modify(sym index.StringIter, set storage.ChunkSeriesSet, log printChangeLog) (index.StringIter, storage.ChunkSeriesSet)
}

type DeletionModifier struct {
	deletions []DeleteRequest
}

func WithDeletionModifier(deletions []DeleteRequest) *DeletionModifier {
	return &DeletionModifier{deletions: deletions}
}

func (d *DeletionModifier) Modify(sym index.StringIter, set storage.ChunkSeriesSet, log printChangeLog) (index.StringIter, storage.ChunkSeriesSet) {
	return sym, &delModifierSeriesSet{
		d: d,

		ChunkSeriesSet: set,
		log:            log,
	}
}

type delModifierSeriesSet struct {
	storage.ChunkSeriesSet

	d   *DeletionModifier
	log printChangeLog

	err error
}

func (d *delModifierSeriesSet) Next() bool {
	for d.ChunkSeriesSet.Next() {
		s := d.ChunkSeriesSet.At()
		lbls := s.Labels()

		var intervals tombstones.Intervals
		for _, deletions := range d.d.deletions {
			for _, m := range deletions.Matchers {
				v := lbls.Get(m.Name)
				if v == "" {
					continue
				}

				if m.Matches(v) {
					continue
				}
				for _, in := range deletions.intervals {
					intervals = intervals.Add(in)
				}
				break
			}
		}

		if (tombstones.Interval{Mint: math.MinInt64, Maxt: math.MaxInt64}.IsSubrange(intervals)) {
			// Quick path for skipping series completely.
			chksIter := d.ChunkSeriesSet.At().Iterator()
			var chks []chunks.Meta
			for chksIter.Next() {
				chks = append(chks, chksIter.At())
			}
			d.err = chksIter.Err()
			if d.err != nil {
				return false
			}

			deleted := tombstones.Intervals{}
			if len(chks) > 0 {
				deleted.Add(tombstones.Interval{Mint: chks[0].MinTime, Maxt: chks[len(chks)].MaxTime})
			}
			d.log.DeleteSeries(lbls, deleted)
			continue
		}
	}
	return false
}
func (d *delModifierSeriesSet) At() storage.ChunkSeries {

}

func (d *delModifierSeriesSet) Err() error {
	panic("implement me")
}

func (d *delModifierSeriesSet) Warnings() storage.Warnings {
	panic("implement me")
}

// TODO(bwplotka): Add relabelling.

type DeleteRequest struct {
	Matchers  []*labels.Matcher
	intervals tombstones.Intervals
}
