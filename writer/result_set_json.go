package writer

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// WriteResultSetJSON streams iter to out as a single Spanner ResultSet in
// protobuf JSON form. Each row is written before the next row is read.
//
// The JSON contains the initial metadata when available, the rows as arrays
// of cells from [spanner.Row.ColumnValue], and stats reconstructed by
// [RowIteratorResult.StatsProto]. Changes to metadata during iteration cause an error.
// Metadata can include a transaction ID. The iterator exposes neither a
// precommit token nor a cache update for this output.
//
// Use [StatsEncodingDMLExact] for completed standard DML to retain an exact
// zero row count. The iterator cannot distinguish a partitioned DML lower
// bound from an exact count, so this output cannot preserve that distinction.
//
// With valid arguments, WriteResultSetJSON takes ownership of iter and stops
// it on return, as in [RunRowIterator]. The caller owns out and its flushing
// or closing. On an error, out may contain a partial document. A nil error
// confirms that the complete document was written to out.
//
// To include query stats, pass an iterator from QueryWithStats:
//
//	result, err := WriteResultSetJSON(txn.QueryWithStats(ctx, stmt), out, StatsEncodingDefault)
func WriteResultSetJSON(iter *spanner.RowIterator, out io.Writer, encoding StatsEncoding) (*RowIteratorResult, error) {
	if iter == nil {
		return nil, ErrNilRowIterator
	}
	if out == nil {
		return nil, ErrNilWriter
	}
	return RunRowIterator(iter, resultSetJSONHooks(out, encoding))
}

func resultSetJSONHooks(out io.Writer, encoding StatsEncoding) RowIteratorHooks {
	encoder := &resultSetJSONEncoder{out: out}
	return NewRowIteratorHooks().
		WithPrepareMetadata(encoder.prepare).
		WithWriteRow(encoder.writeRow).
		WithFinish(func(result *RowIteratorResult) error {
			return encoder.finish(result, encoding)
		})
}

type resultSetJSONEncoder struct {
	out      io.Writer
	buffer   bytes.Buffer
	wroteRow bool
	metadata *sppb.ResultSetMetadata
}

func (e *resultSetJSONEncoder) prepare(metadata *sppb.ResultSetMetadata) error {
	e.metadata = metadata
	e.buffer.WriteByte('{')
	if metadata != nil {
		e.buffer.WriteString(`"metadata":`)
		if err := e.appendProto(metadata); err != nil {
			return fmt.Errorf("encode ResultSet metadata: %w", err)
		}
		e.buffer.WriteByte(',')
	}
	e.buffer.WriteString(`"rows":[`)
	return e.write(e.buffer.Bytes())
}

func (e *resultSetJSONEncoder) writeRow(row *spanner.Row) error {
	if row == nil {
		return ErrNilRow
	}
	e.buffer.Reset()
	if e.wroteRow {
		e.buffer.WriteByte(',')
	}
	e.buffer.WriteByte('[')
	for i := range row.Size() {
		if i > 0 {
			e.buffer.WriteByte(',')
		}
		value := row.ColumnValue(i)
		if value == nil {
			return fmt.Errorf("encode ResultSet column %d: missing wire value", i)
		}
		if err := e.appendProto(value); err != nil {
			return fmt.Errorf("encode ResultSet column %d: %w", i, err)
		}
	}
	e.buffer.WriteByte(']')
	if err := e.write(e.buffer.Bytes()); err != nil {
		return err
	}
	e.wroteRow = true
	return nil
}

func (e *resultSetJSONEncoder) finish(result *RowIteratorResult, encoding StatsEncoding) error {
	if !proto.Equal(e.metadata, result.Metadata) {
		return errors.New("ResultSet metadata changed during iteration")
	}
	stats, err := result.StatsProto(encoding)
	if err != nil {
		return err
	}
	e.buffer.Reset()
	e.buffer.WriteByte(']')
	if stats != nil {
		e.buffer.WriteString(`,"stats":`)
		if err := e.appendProto(stats); err != nil {
			return fmt.Errorf("encode ResultSet stats: %w", err)
		}
	}
	e.buffer.WriteByte('}')
	return e.write(e.buffer.Bytes())
}

func (e *resultSetJSONEncoder) appendProto(message proto.Message) error {
	data, err := protojson.Marshal(message)
	if err != nil {
		return err
	}
	return json.Compact(&e.buffer, data)
}

func (e *resultSetJSONEncoder) write(data []byte) error {
	n, err := e.out.Write(data)
	if err == nil && n != len(data) {
		return io.ErrShortWrite
	}
	return err
}
