package filter

import (
	"encoding/json"
	"github.com/uwork/bingo/mysql/binlog"
)

type Filter struct {
	Database string     `json:"database"`
	Table    string     `json:"table"`
	Columns  []int      `json:"columns"`
	Where    Expression `json:"where"`
}

type FilteredRow struct {
	Database string   `json:"database"`
	Table    string   `json:"table"`
	Columns  []string `json:"columns"`
}

func NewFilteredRow(row binlog.Row) FilteredRow {
	fr := FilteredRow{}
	fr.Columns = []string{}
	for _, c := range row.Columns {
		fr.Columns = append(fr.Columns, c.String())
	}
	return fr
}

type FilterConfig struct {
	Filters []Filter `json:"filters"`
}

func (f *FilterConfig) FilterEvent(ev *binlog.BinlogEvent) ([]byte, error) {
	rows := []binlog.Row{}
	if 0 == len(f.Filters) {
		for _, row := range ev.Rows.Rows {
			rows = append(rows, row)
		}
	} else {
		// filter condition
		for _, filter := range f.Filters {
			if 0 < len(filter.Database) && ev.Rows.Schema != filter.Database {
				continue
			}
			if 0 < len(filter.Table) && ev.Rows.Table != filter.Table {
				continue
			}

			for _, row := range ev.Rows.Rows {
				match := true
				if filter.Where.Op != "" {
					if _match, err := filter.Where.Evaluate(row); err != nil {
						return nil, err
					} else {
						match = _match
					}
				}

				if match {
					if 0 == len(filter.Columns) {
						rows = append(rows, row)
					} else {
						newRow := row
						newRow.Columns = []binlog.Column{}
						for _, col := range filter.Columns {
							newRow.Columns = append(newRow.Columns, row.Columns[col])
						}
						rows = append(rows, newRow)
					}
				}
			}
		}
	}

	if 0 < len(rows) {
		frows := []FilteredRow{}
		for _, row := range rows {
			frow := NewFilteredRow(row)
			frow.Database = ev.Rows.Schema
			frow.Table = ev.Rows.Table
			frows = append(frows, frow)
		}

		bin, err := json.Marshal(frows)
		if err != nil {
			return nil, err
		}
		return bin, nil
	}
	return nil, nil
}
