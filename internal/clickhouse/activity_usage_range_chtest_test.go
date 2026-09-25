//go:build chtest

package clickhouse

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.kenn.io/agentsview/internal/activity"
	"go.kenn.io/agentsview/internal/db"
)

// A range still in progress keeps every session's usage rows. After a push
// its report re-reads only the pushed session, whether the pushed rows are
// still in the read-time delta or already refreshed, and still matches the
// local archive, including the Claude peers of a filtered report.
func TestPartialActivityUsageRereadsOnlyPushedSessions(t *testing.T) {
	store, syncer, local := newPushedStore(t)
	ctx := t.Context()
	const peerID = "range-peer"
	started := time.Date(2026, 1, 10, 1, 0, 0, 0, time.UTC)
	messageID := func(session, ordinal int) string { return fmt.Sprintf("msg-%d-%d", session, ordinal) }
	requestID := func(session, ordinal int) string { return fmt.Sprintf("req-%d-%d", session, ordinal) }
	var writes []db.SessionBatchWrite
	for i := range 3 {
		id := fmt.Sprintf("range-candidate-%d", i)
		msgs := []db.Message{fixtureMessage(id, 0, "user", "candidate", started.Format(time.RFC3339Nano))}
		for j := range 4 {
			m := fixtureMessage(id, j+1, "assistant", "reply", started.Add(time.Duration(i*10+j+1)*time.Second).Format(time.RFC3339Nano))
			m.ClaudeMessageID, m.ClaudeRequestID = messageID(i, j), requestID(i, j)
			msgs = append(msgs, m)
		}
		writes = append(writes, db.SessionBatchWrite{
			Session:  fixtureSession(id, "range", "candidate", started.Format(time.RFC3339Nano), len(msgs)),
			Messages: msgs, DataVersion: 1, ReplaceMessages: true,
		})
	}
	// The peer is outside the project filter. Its copies of the first
	// candidate's replies carry more output tokens, so survivor selection
	// must pick them.
	peerMessages := func(copies int) []db.Message {
		msgs := []db.Message{fixtureMessage(peerID, 0, "user", "peer", started.Format(time.RFC3339Nano))}
		for j := range copies {
			m := fixtureMessage(peerID, j+1, "assistant", "reply", started.Add(time.Duration(j+2)*time.Second).Format(time.RFC3339Nano))
			m.ClaudeMessageID, m.ClaudeRequestID = messageID(0, j), requestID(0, j)
			m.TokenUsage = []byte(`{"input_tokens":1,"output_tokens":5}`)
			msgs = append(msgs, m)
		}
		return msgs
	}
	writePeer := func(copies int) {
		msgs := peerMessages(copies)
		ended := started.Add(time.Duration(copies+2) * time.Second).Format(time.RFC3339Nano)
		peer := fixtureSession(peerID, "elsewhere", "peer", started.Format(time.RFC3339Nano), len(msgs))
		peer.EndedAt, peer.LocalModifiedAt = &ended, &ended
		_, err := local.WriteSessionBatchAtomic(ctx, []db.SessionBatchWrite{{
			Session: peer, Messages: msgs, DataVersion: 1, ReplaceMessages: true,
		}})
		require.NoError(t, err)
	}
	_, err := local.WriteSessionBatchAtomic(ctx, writes)
	require.NoError(t, err)
	writePeer(2)
	_, err = syncer.Push(ctx, false, nil)
	require.NoError(t, err)
	_, err = store.DB().ExecContext(ctx, "SYSTEM WAIT VIEW prepare_usage")
	require.NoError(t, err)

	now := time.Now()
	q, err := activity.ResolveQuery(activity.QueryInput{
		Preset: "custom", From: "2026-01-01T00:00:00Z", To: now.AddDate(0, 0, 2).UTC().Format(time.RFC3339), Timezone: "UTC",
	}, now)
	require.NoError(t, err)
	require.True(t, q.Partial)
	filtered := db.AnalyticsFilter{Timezone: "UTC", Project: "range"}
	everything := db.AnalyticsFilter{Timezone: "UTC"}
	requireParity := func(label string) {
		t.Helper()
		for _, f := range []db.AnalyticsFilter{filtered, everything} {
			want, err := local.GetActivityReport(ctx, f, q)
			require.NoError(t, err)
			got, err := store.GetActivityReport(ctx, f, q)
			require.NoError(t, err)
			require.Equal(t, want.Totals.OutputTokens, got.Totals.OutputTokens, "%s, project %q", label, f.Project)
			require.Equal(t, want.Totals.Cost, got.Totals.Cost, "%s, project %q", label, f.Project)
			require.Equal(t, want.Totals.Sessions, got.Totals.Sessions, "%s, project %q", label, f.Project)
		}
	}
	requireParity("first read")
	first := store.activityUsageRowsRead.Load()
	require.Positive(t, first)

	// With the refresh stopped, the pushed peer's rows come from the delta
	// prepared at read time.
	_, err = store.DB().ExecContext(ctx, "SYSTEM STOP VIEW prepare_usage")
	require.NoError(t, err)
	writePeer(4)
	_, err = syncer.Push(ctx, false, nil)
	require.NoError(t, err)
	requireParity("delta")
	reread := store.activityUsageRowsRead.Load() - first
	require.Equal(t, int64(len(peerMessages(4))), reread, "only the pushed peer's rows are read again")

	// The refresh stores the same rows the delta supplied, so nothing is
	// read again.
	_, err = store.DB().ExecContext(ctx, "SYSTEM START VIEW prepare_usage")
	require.NoError(t, err)
	_, err = store.DB().ExecContext(ctx, "SYSTEM REFRESH VIEW prepare_usage")
	require.NoError(t, err)
	_, err = store.DB().ExecContext(ctx, "SYSTEM WAIT VIEW prepare_usage")
	require.NoError(t, err)
	before := store.activityUsageRowsRead.Load()
	requireParity("refreshed")
	require.Equal(t, before, store.activityUsageRowsRead.Load(), "a refresh alone reads no rows")
}
