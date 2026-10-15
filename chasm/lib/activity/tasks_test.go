package activity

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/retrypolicy"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestActivityDispatchTaskHook(t *testing.T) {
	t.Run("wraps active dispatch", func(t *testing.T) {
		controller := gomock.NewController(t)
		engine := chasm.NewMockEngine(controller)
		matchingClient := matchingservicemock.NewMockMatchingServiceClient(controller)
		activityRef := chasm.NewComponentRef[*Activity](chasm.ExecutionKey{NamespaceID: "namespace-id"})
		task := &activitypb.ActivityDispatchTask{DispatchReason: activitypb.DISPATCH_REASON_RETRY}
		chasmCtx := &chasm.MockMutableContext{
			MockContext: chasm.MockContext{
				HandleRef: func(chasm.Component) ([]byte, error) {
					return []byte("component-ref"), nil
				},
			},
		}
		activity := &Activity{
			ActivityState: &activitypb.ActivityState{
				TaskQueue: &taskqueuepb.TaskQueue{Name: "task-queue"},
			},
			LastAttempt: chasm.NewDataField(chasmCtx, &activitypb.ActivityAttemptState{Stamp: 7}),
		}

		engine.EXPECT().ReadComponent(gomock.Any(), activityRef, gomock.Any()).DoAndReturn(
			func(
				_ context.Context,
				_ chasm.ComponentRef,
				readFn func(chasm.Context, chasm.Component) error,
				_ ...chasm.TransitionOption,
			) error {
				return readFn(chasmCtx, activity)
			},
		)
		matchingClient.EXPECT().AddActivityTask(gomock.Any(), gomock.Any()).Return(nil, nil)

		hookCalls := 0
		handler := newActivityDispatchTaskHandler(activityDispatchTaskHandlerOptions{
			MatchingClient: matchingClient,
			DispatchTaskHook: func(
				ctx context.Context,
				namespaceID string,
				actualTask *activitypb.ActivityDispatchTask,
				dispatch func(context.Context) error,
			) error {
				hookCalls++
				require.Equal(t, "namespace-id", namespaceID)
				require.Same(t, task, actualTask)

				return dispatch(ctx)
			},
		})

		err := handler.Execute(chasm.NewEngineContext(context.Background(), engine), activityRef, chasm.TaskAttributes{}, task)
		require.NoError(t, err)
		require.Equal(t, 1, hookCalls)
	})

	t.Run("honors hook result", func(t *testing.T) {
		hookErr := errors.New("hook failed")
		testCases := []struct {
			name    string
			hookErr error
		}{
			{name: "suppresses dispatch"},
			{name: "propagates error", hookErr: hookErr},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				controller := gomock.NewController(t)
				engine := chasm.NewMockEngine(controller)
				activityRef := chasm.NewComponentRef[*Activity](chasm.ExecutionKey{NamespaceID: "namespace-id"})
				chasmCtx := &chasm.MockMutableContext{
					MockContext: chasm.MockContext{
						HandleRef: func(chasm.Component) ([]byte, error) {
							return []byte("component-ref"), nil
						},
					},
				}
				activity := &Activity{
					ActivityState: &activitypb.ActivityState{
						TaskQueue: &taskqueuepb.TaskQueue{Name: "task-queue"},
					},
					LastAttempt: chasm.NewDataField(chasmCtx, &activitypb.ActivityAttemptState{Stamp: 7}),
				}

				engine.EXPECT().ReadComponent(gomock.Any(), activityRef, gomock.Any()).DoAndReturn(
					func(
						_ context.Context,
						_ chasm.ComponentRef,
						readFn func(chasm.Context, chasm.Component) error,
						_ ...chasm.TransitionOption,
					) error {
						return readFn(chasmCtx, activity)
					},
				)

				handler := newActivityDispatchTaskHandler(activityDispatchTaskHandlerOptions{
					MatchingClient: matchingservicemock.NewMockMatchingServiceClient(controller),
					DispatchTaskHook: func(
						context.Context,
						string,
						*activitypb.ActivityDispatchTask,
						func(context.Context) error,
					) error {
						return tc.hookErr
					},
				})

				err := handler.Execute(
					chasm.NewEngineContext(context.Background(), engine),
					activityRef,
					chasm.TaskAttributes{},
					&activitypb.ActivityDispatchTask{},
				)
				if tc.hookErr == nil {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, tc.hookErr)
				}
			})
		}
	})

	t.Run("skipped when request construction fails", func(t *testing.T) {
		controller := gomock.NewController(t)
		engine := chasm.NewMockEngine(controller)
		activityRef := chasm.NewComponentRef[*Activity](chasm.ExecutionKey{NamespaceID: "namespace-id"})
		validationErr := serviceerror.NewNotFound("task is no longer valid")

		engine.EXPECT().ReadComponent(gomock.Any(), activityRef, gomock.Any()).Return(validationErr)

		hookCalls := 0
		handler := newActivityDispatchTaskHandler(activityDispatchTaskHandlerOptions{
			MatchingClient: matchingservicemock.NewMockMatchingServiceClient(controller),
			DispatchTaskHook: func(
				context.Context,
				string,
				*activitypb.ActivityDispatchTask,
				func(context.Context) error,
			) error {
				hookCalls++
				return nil
			},
		})

		err := handler.Execute(
			chasm.NewEngineContext(context.Background(), engine),
			activityRef,
			chasm.TaskAttributes{},
			&activitypb.ActivityDispatchTask{},
		)
		require.ErrorIs(t, err, validationErr)
		require.Zero(t, hookCalls)
	})

	t.Run("bypassed on standby discard", func(t *testing.T) {
		controller := gomock.NewController(t)
		engine := chasm.NewMockEngine(controller)
		matchingClient := matchingservicemock.NewMockMatchingServiceClient(controller)
		activityRef := chasm.NewComponentRef[*Activity](chasm.ExecutionKey{NamespaceID: "namespace-id"})
		chasmCtx := &chasm.MockMutableContext{
			MockContext: chasm.MockContext{
				HandleRef: func(chasm.Component) ([]byte, error) {
					return []byte("component-ref"), nil
				},
			},
		}
		activity := &Activity{
			ActivityState: &activitypb.ActivityState{
				TaskQueue: &taskqueuepb.TaskQueue{Name: "task-queue"},
			},
			LastAttempt: chasm.NewDataField(chasmCtx, &activitypb.ActivityAttemptState{Stamp: 7}),
		}

		engine.EXPECT().ReadComponent(gomock.Any(), activityRef, gomock.Any()).DoAndReturn(
			func(
				_ context.Context,
				_ chasm.ComponentRef,
				readFn func(chasm.Context, chasm.Component) error,
				_ ...chasm.TransitionOption,
			) error {
				return readFn(chasmCtx, activity)
			},
		)
		matchingClient.EXPECT().AddActivityTask(gomock.Any(), gomock.Any()).Return(nil, nil)

		hookCalls := 0
		handler := newActivityDispatchTaskHandler(activityDispatchTaskHandlerOptions{
			MatchingClient: matchingClient,
			DispatchTaskHook: func(
				context.Context,
				string,
				*activitypb.ActivityDispatchTask,
				func(context.Context) error,
			) error {
				hookCalls++
				return nil
			},
		})

		err := handler.Discard(
			chasm.NewEngineContext(context.Background(), engine),
			activityRef,
			chasm.TaskAttributes{},
			&activitypb.ActivityDispatchTask{DispatchReason: activitypb.DISPATCH_REASON_RETRY},
		)
		require.NoError(t, err)
		require.Zero(t, hookCalls)
	})
}

func TestActivityIsDispatchTaskValid(t *testing.T) {
	const currentStamp int32 = 42

	testCases := []struct {
		name   string
		status activitypb.ActivityExecutionStatus
		stamp  int32
		valid  bool
	}{
		{
			name:   "scheduled current attempt",
			status: activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
			stamp:  currentStamp,
			valid:  true,
		},
		{
			name:   "scheduled obsolete attempt",
			status: activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
			stamp:  currentStamp - 1,
			valid:  false,
		},
		{
			name:   "scheduled future attempt",
			status: activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
			stamp:  currentStamp + 1,
			valid:  false,
		},
		{
			name:   "started",
			status: activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
			stamp:  currentStamp,
			valid:  false,
		},
		{
			name:   "paused",
			status: activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED,
			stamp:  currentStamp,
			valid:  false,
		},
		{
			name:   "completed",
			status: activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED,
			stamp:  currentStamp,
			valid:  false,
		},
		// A terminated/closed execution must never dispatch an activity task, even when the
		// persisted task carries the current attempt stamp (e.g. a retry dispatch in flight when
		// the execution was terminated).
		{
			name:   "canceled",
			status: activitypb.ACTIVITY_EXECUTION_STATUS_CANCELED,
			stamp:  currentStamp,
			valid:  false,
		},
		{
			name:   "failed",
			status: activitypb.ACTIVITY_EXECUTION_STATUS_FAILED,
			stamp:  currentStamp,
			valid:  false,
		},
		{
			name:   "timed out",
			status: activitypb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
			stamp:  currentStamp,
			valid:  false,
		},
		{
			name:   "terminated",
			status: activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED,
			stamp:  currentStamp,
			valid:  false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &chasm.MockMutableContext{}
			activity := &Activity{
				ActivityState: &activitypb.ActivityState{Status: tc.status},
				LastAttempt: chasm.NewDataField(ctx, &activitypb.ActivityAttemptState{
					Stamp: currentStamp,
				}),
			}

			valid, err := activity.IsDispatchTaskValid(ctx, tc.stamp)

			require.NoError(t, err)
			require.Equal(t, tc.valid, valid)
		})
	}
}

func TestScheduleToCloseTimeoutTaskValidateStamp(t *testing.T) {
	handler := newScheduleToCloseTimeoutTaskHandler()

	newActivity := func(stamp int32) *Activity {
		return &Activity{
			ActivityState: &activitypb.ActivityState{
				Status:                 activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
				ScheduleToCloseTimeout: durationpb.New(30 * time.Second),
				ScheduleToCloseStamp:   stamp,
			},
		}
	}

	testCases := []struct {
		name          string
		activityStamp int32
		taskStamp     int32
		wantValid     bool
	}{
		// A legacy activity (created before ScheduleToCloseStamp existed) has stamp 0 on both
		// the state and the timer task. Its timer is valid only while the activity stamp is
		// still 0; once an options update bumps the stamp, the zero-stamp timer must be rejected.
		{"legacy task valid while activity stamp still zero", 0, 0, true},
		{"legacy zero-stamp task invalid after stamp bumped", 5, 0, false},
		{"matching stamp valid", 5, 5, true},
		{"stale non-zero stamp invalid", 5, 3, false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			valid, err := handler.Validate(
				nil,
				newActivity(tc.activityStamp),
				chasm.TaskInvocation{},
				&activitypb.ScheduleToCloseTimeoutTask{Stamp: tc.taskStamp},
			)
			require.NoError(t, err)
			require.Equal(t, tc.wantValid, valid)
		})
	}
}

func TestTimeoutTaskTerminalFailure(t *testing.T) {
	testCases := []struct {
		name                string
		timeoutType         enumspb.TimeoutType
		startStatus         activitypb.ActivityExecutionStatus
		maximumAttempts     int32
		scheduleToClose     time.Duration
		nonRetryable        bool
		expectedTimeoutType enumspb.TimeoutType
		expectedRetryState  enumspb.RetryState
		expectedMessage     string
	}{
		{
			name:                "start to close deadline exhausted",
			timeoutType:         enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			scheduleToClose:     2 * time.Second,
			expectedTimeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
			expectedRetryState:  enumspb.RETRY_STATE_TIMEOUT,
			expectedMessage:     common.FailureReasonActivityRetryScheduleToCloseTimeout,
		},
		{
			name:                "heartbeat deadline exhausted",
			timeoutType:         enumspb.TIMEOUT_TYPE_HEARTBEAT,
			scheduleToClose:     2 * time.Second,
			expectedTimeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
			expectedRetryState:  enumspb.RETRY_STATE_TIMEOUT,
			expectedMessage:     common.FailureReasonActivityRetryScheduleToCloseTimeout,
		},
		{
			name:                "start to close maximum attempts reached",
			timeoutType:         enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			maximumAttempts:     1,
			scheduleToClose:     time.Minute,
			expectedTimeoutType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			expectedRetryState:  enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED,
		},
		{
			name:                "heartbeat maximum attempts reached",
			timeoutType:         enumspb.TIMEOUT_TYPE_HEARTBEAT,
			maximumAttempts:     1,
			scheduleToClose:     time.Minute,
			expectedTimeoutType: enumspb.TIMEOUT_TYPE_HEARTBEAT,
			expectedRetryState:  enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED,
		},
		{
			name:                "start to close non-retryable timeout",
			timeoutType:         enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			scheduleToClose:     time.Minute,
			nonRetryable:        true,
			expectedTimeoutType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			expectedRetryState:  enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE,
		},
		{
			name:                "start to close cancellation requested",
			timeoutType:         enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			startStatus:         activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
			scheduleToClose:     time.Minute,
			expectedTimeoutType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			expectedRetryState:  enumspb.RETRY_STATE_CANCEL_REQUESTED,
		},
		{
			// A worker that stops heartbeating after cancellation was requested must close as
			// CANCEL_REQUESTED rather than be rescheduled by the server: worker-driven cancellation
			// takes precedence over server-side retry even though the triggering timer is a
			// heartbeat timeout.
			name:                "heartbeat cancellation requested",
			timeoutType:         enumspb.TIMEOUT_TYPE_HEARTBEAT,
			startStatus:         activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
			scheduleToClose:     time.Minute,
			expectedTimeoutType: enumspb.TIMEOUT_TYPE_HEARTBEAT,
			expectedRetryState:  enumspb.RETRY_STATE_CANCEL_REQUESTED,
		},
		{
			name:                "schedule to close cancellation requested",
			timeoutType:         enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
			startStatus:         activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
			scheduleToClose:     time.Minute,
			expectedTimeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
			expectedRetryState:  enumspb.RETRY_STATE_CANCEL_REQUESTED,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			metricsHandler := metricstest.NewCaptureHandler()
			capture := metricsHandler.StartCapture()
			defer metricsHandler.StopCapture(capture)
			ctx := &chasm.MockMutableContext{
				MockContext: chasm.MockContext{
					HandleNow:            func(chasm.Component) time.Time { return defaultTime.Add(1500 * time.Millisecond) },
					HandleMetricsHandler: func() metrics.Handler { return metricsHandler },
					HandleNamespaceEntry: testNamespaceEntry,
					GoCtx: context.WithValue(context.Background(), ctxKeyActivityContext, &activityContext{
						config: &Config{
							BreakdownMetricsByTaskQueue: dynamicconfig.GetBoolPropertyFnFilteredByTaskQueue(false),
						},
					}),
				},
			}
			retryPolicy := &commonpb.RetryPolicy{
				InitialInterval:    durationpb.New(time.Second),
				BackoffCoefficient: 1,
				MaximumAttempts:    tc.maximumAttempts,
			}
			if tc.nonRetryable {
				retryPolicy.NonRetryableErrorTypes = []string{
					retrypolicy.TimeoutFailureTypePrefix + tc.timeoutType.String(),
				}
			}
			startStatus := tc.startStatus
			if startStatus == activitypb.ACTIVITY_EXECUTION_STATUS_UNSPECIFIED {
				startStatus = activitypb.ACTIVITY_EXECUTION_STATUS_STARTED
			}
			activity := &Activity{
				ActivityState: &activitypb.ActivityState{
					ActivityType:           &commonpb.ActivityType{Name: "test-activity-type"},
					RetryPolicy:            retryPolicy,
					ScheduleTime:           timestamppb.New(defaultTime),
					ScheduleToCloseTimeout: durationpb.New(tc.scheduleToClose),
					Status:                 startStatus,
					TaskQueue:              &taskqueuepb.TaskQueue{Name: "test-task-queue"},
				},
				LastAttempt: chasm.NewDataField(ctx, &activitypb.ActivityAttemptState{
					Count:       1,
					StartedTime: timestamppb.New(defaultTime),
				}),
				Outcome: chasm.NewDataField(ctx, &activitypb.ActivityOutcome{}),
				LastHeartbeat: chasm.NewDataField(ctx, &activitypb.ActivityHeartbeatState{
					Details: &commonpb.Payloads{Payloads: []*commonpb.Payload{{Data: []byte("heartbeat details")}}},
				}),
			}

			var err error
			switch tc.timeoutType {
			case enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE:
				err = newScheduleToCloseTimeoutTaskHandler().Execute(ctx, activity, chasm.TaskAttributes{}, &activitypb.ScheduleToCloseTimeoutTask{})
			case enumspb.TIMEOUT_TYPE_START_TO_CLOSE:
				err = newStartToCloseTimeoutTaskHandler().Execute(ctx, activity, chasm.TaskAttributes{}, &activitypb.StartToCloseTimeoutTask{})
			case enumspb.TIMEOUT_TYPE_HEARTBEAT:
				err = newHeartbeatTimeoutTaskHandler().Execute(ctx, activity, chasm.TaskAttributes{}, &activitypb.HeartbeatTimeoutTask{})
			default:
				t.Fatalf("unexpected timeout type: %v", tc.timeoutType)
			}
			require.NoError(t, err)

			terminalFailure := activity.outcome(ctx).GetFailure()
			require.Equal(t, tc.expectedTimeoutType, terminalFailure.GetTimeoutFailureInfo().GetTimeoutType())
			require.Equal(t, tc.expectedRetryState, activity.outcome(ctx).GetRetryState())
			if tc.expectedMessage != "" {
				require.Equal(t, tc.expectedMessage, terminalFailure.GetMessage())
			}
			if tc.timeoutType == enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE || tc.expectedMessage != "" {
				require.NotNil(t, activity.Outcome.Get(ctx).GetFailed())
			} else {
				require.Nil(t, activity.Outcome.Get(ctx).GetVariant())
			}
			if tc.timeoutType == enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE {
				require.Nil(t, activity.LastAttempt.Get(ctx).GetLastFailureDetails())
			} else {
				lastFailure := activity.LastAttempt.Get(ctx).GetLastFailureDetails().GetFailure()
				require.Equal(t, tc.timeoutType, lastFailure.GetTimeoutFailureInfo().GetTimeoutType())
				require.Equal(t, lastFailure.GetTimeoutFailureInfo().GetLastHeartbeatDetails(), terminalFailure.GetTimeoutFailureInfo().GetLastHeartbeatDetails())
			}

			for _, metricName := range []string{metrics.ActivityTimeout.Name(), metrics.ActivityTaskTimeout.Name()} {
				recordings := capture.Snapshot()[metricName]
				require.Len(t, recordings, 1)
				require.Equal(t, tc.timeoutType.String(), recordings[0].Tags["timeout_type"])
			}
		})
	}
}

// TestHeartbeatTimeoutTaskValidate covers the coordination between worker heartbeats and the
// heartbeat timeout timer:
//   - a timer for which no newer heartbeat arrived (including the initial timer anchored at the
//     attempt start) is valid;
//   - a heartbeat arriving after the timer was scheduled invalidates it, so a late/out-of-order
//     timer delivery (network jitter) can no longer time out a live attempt;
//   - the check is based on the timer's persisted scheduled time, not wall-clock time, so it
//     stays deterministic on shard restart / catch-up replay;
//   - timers from an old attempt stamp (server already rescheduled) and timers of closed
//     executions are discarded;
//   - the timer keeps firing while the worker still owns the token under a cancel/pause/reset
//     request, so a worker that goes silent is still reaped.
func TestHeartbeatTimeoutTaskValidate(t *testing.T) {
	const (
		currentAttemptStamp int32 = 1
		hbTimeout                 = 10 * time.Second
	)
	startedAt := defaultTime
	handler := newHeartbeatTimeoutTaskHandler()

	type testCase struct {
		name          string
		status        activitypb.ActivityExecutionStatus
		attemptStamp  int32
		lastHeartbeat *time.Time
		// taskScheduledTime is the persisted scheduled time carried by the timer task (hb_i + T).
		taskScheduledTime time.Time
		// now is wall-clock time at validation. Validation must not depend on it.
		now          time.Time
		wantValid    bool
	}
	hbTime := func(offset time.Duration) time.Time { return startedAt.Add(offset) }

	testCases := []testCase{
		{
			name:              "initial heartbeat timer valid without any heartbeat",
			status:            activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
			attemptStamp:      currentAttemptStamp,
			taskScheduledTime: hbTime(hbTimeout),
			now:               hbTime(hbTimeout),
			wantValid:         true,
		},
		{
			name:              "newer heartbeat invalidates the initial timer",
			status:            activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
			attemptStamp:      currentAttemptStamp,
			lastHeartbeat:     ptr(hbTime(6 * time.Second)),
			taskScheduledTime: hbTime(hbTimeout),
			now:               hbTime(hbTimeout),
			wantValid:         false,
		},
		{
			name:              "heartbeat exactly at the timer threshold keeps it valid",
			status:            activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
			attemptStamp:      currentAttemptStamp,
			lastHeartbeat:     ptr(hbTime(6 * time.Second)),
			taskScheduledTime: hbTime(16 * time.Second),
			now:               hbTime(16 * time.Second),
			wantValid:         true,
		},
		{
			name:              "stale attempt stamp discarded after server-side reschedule",
			status:            activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
			attemptStamp:      currentAttemptStamp + 1,
			lastHeartbeat:     ptr(hbTime(6 * time.Second)),
			taskScheduledTime: hbTime(hbTimeout),
			now:               hbTime(hbTimeout),
			wantValid:         false,
		},
		{
			name:              "cancel requested timer still enforced while worker holds the token",
			status:            activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
			attemptStamp:      currentAttemptStamp,
			lastHeartbeat:     ptr(hbTime(6 * time.Second)),
			taskScheduledTime: hbTime(16 * time.Second),
			now:               hbTime(16 * time.Second),
			wantValid:         true,
		},
		{
			name:              "pause requested timer still enforced",
			status:            activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED,
			attemptStamp:      currentAttemptStamp,
			lastHeartbeat:     ptr(hbTime(6 * time.Second)),
			taskScheduledTime: hbTime(16 * time.Second),
			now:               hbTime(16 * time.Second),
			wantValid:         true,
		},
		{
			name:              "reset requested timer still enforced",
			status:            activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED,
			attemptStamp:      currentAttemptStamp,
			lastHeartbeat:     ptr(hbTime(6 * time.Second)),
			taskScheduledTime: hbTime(16 * time.Second),
			now:               hbTime(16 * time.Second),
			wantValid:         true,
		},
		{
			name:              "closed execution timer discarded",
			status:            activitypb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
			attemptStamp:      currentAttemptStamp,
			lastHeartbeat:     ptr(hbTime(6 * time.Second)),
			taskScheduledTime: hbTime(16 * time.Second),
			now:               hbTime(16 * time.Second),
			wantValid:         false,
		},
		{
			// Process restart: the task fires long after its persisted scheduled time. The
			// decision must still be derived from persisted heartbeat state only.
			name:              "restart catch-up replays against persisted scheduled time",
			status:            activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
			attemptStamp:      currentAttemptStamp,
			lastHeartbeat:     ptr(hbTime(6 * time.Second)),
			taskScheduledTime: hbTime(16 * time.Second),
			now:               hbTime(time.Hour),
			wantValid:         true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &chasm.MockMutableContext{
				MockContext: chasm.MockContext{
					HandleNow: func(chasm.Component) time.Time { return tc.now },
				},
			}
			activity := &Activity{
				ActivityState: &activitypb.ActivityState{
					Status:           tc.status,
					HeartbeatTimeout: durationpb.New(hbTimeout),
				},
				LastAttempt: chasm.NewDataField(ctx, &activitypb.ActivityAttemptState{
					Count:        1,
					Stamp:        tc.attemptStamp,
					StartedStamp: tc.attemptStamp,
					StartedTime:  timestamppb.New(startedAt),
				}),
			}
			if tc.lastHeartbeat != nil {
				activity.LastHeartbeat = chasm.NewDataField(ctx, &activitypb.ActivityHeartbeatState{
					RecordedTime: timestamppb.New(*tc.lastHeartbeat),
				})
			}

			valid, err := handler.Validate(
				ctx,
				activity,
				chasm.TaskInvocation{TaskAttributes: chasm.TaskAttributes{ScheduledTime: tc.taskScheduledTime}},
				&activitypb.HeartbeatTimeoutTask{Stamp: currentAttemptStamp},
			)
			require.NoError(t, err)
			require.Equal(t, tc.wantValid, valid)
		})
	}
}

// TestHeartbeatTimeoutReschedulesAndDuplicateTaskDropped verifies that a heartbeat timeout with
// retries left is a server-side reschedule (attempt and stamp bumped, retry dispatch emitted, no
// terminal outcome), and that the duplicate heartbeat-timeout tasks persisted by repeated
// heartbeats all become no-ops once the first one reschedules the attempt.
func TestHeartbeatTimeoutReschedulesAndDuplicateTaskDropped(t *testing.T) {
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	defer metricsHandler.StopCapture(capture)

	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow:            func(chasm.Component) time.Time { return defaultTime.Add(1500 * time.Millisecond) },
			HandleMetricsHandler: func() metrics.Handler { return metricsHandler },
			HandleNamespaceEntry: testNamespaceEntry,
			GoCtx: context.WithValue(context.Background(), ctxKeyActivityContext, &activityContext{
				config: &Config{
					BreakdownMetricsByTaskQueue:              dynamicconfig.GetBoolPropertyFnFilteredByTaskQueue(false),
					MutableStateActivityFailureSizeLimitError: dynamicconfig.GetIntPropertyFnFilteredByNamespace(defaultFailureSizeLimit),
				},
			}),
		},
	}

	const stamp int32 = 1
	activity := &Activity{
		ActivityState: &activitypb.ActivityState{
			ActivityType: &commonpb.ActivityType{Name: "test-activity-type"},
			TaskQueue:    &taskqueuepb.TaskQueue{Name: "test-task-queue"},
			RetryPolicy: &commonpb.RetryPolicy{
				InitialInterval:    durationpb.New(time.Second),
				BackoffCoefficient: 1,
				MaximumAttempts:    5,
			},
			ScheduleTime:           timestamppb.New(defaultTime),
			ScheduleToCloseTimeout: durationpb.New(10 * time.Minute),
			Status:                 activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
		},
		LastAttempt: chasm.NewDataField(ctx, &activitypb.ActivityAttemptState{
			Count:       1,
			Stamp:       stamp,
			StartedStamp: stamp,
			StartedTime: timestamppb.New(defaultTime),
		}),
		Outcome:       chasm.NewDataField(ctx, &activitypb.ActivityOutcome{}),
		LastHeartbeat: chasm.NewDataField(ctx, &activitypb.ActivityHeartbeatState{}),
	}

	task := &activitypb.HeartbeatTimeoutTask{Stamp: stamp}
	err := newHeartbeatTimeoutTaskHandler().Execute(ctx, activity, chasm.TaskAttributes{}, task)
	require.NoError(t, err)

	// Server-side retry, not a terminal outcome.
	require.Equal(t, activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED, activity.GetStatus())
	attempt := activity.LastAttempt.Get(ctx)
	require.Equal(t, int32(2), attempt.GetCount())
	require.Equal(t, stamp+1, attempt.GetStamp())
	require.NotNil(t, attempt.GetCompleteTime())
	require.Equal(t, time.Second, attempt.GetCurrentRetryInterval().AsDuration())
	require.Nil(t, activity.Outcome.Get(ctx).GetVariant())
	require.Equal(t, enumspb.TIMEOUT_TYPE_HEARTBEAT,
		attempt.GetLastFailureDetails().GetFailure().GetTimeoutFailureInfo().GetTimeoutType())

	// A retry dispatch for the new attempt must have been scheduled with the new stamp.
	var dispatchTask *activitypb.ActivityDispatchTask
	for _, addedTask := range ctx.Tasks {
		if t, ok := addedTask.Payload.(*activitypb.ActivityDispatchTask); ok {
			dispatchTask = t
		}
	}
	require.NotNil(t, dispatchTask)
	require.Equal(t, activitypb.DISPATCH_REASON_RETRY, dispatchTask.GetDispatchReason())
	require.Equal(t, stamp+1, dispatchTask.GetStamp())

	// Duplicate heartbeat-timeout tasks (e.g. repeated heartbeats before the timeout, or
	// transaction retries) must all be discarded now.
	valid, err := newHeartbeatTimeoutTaskHandler().Validate(
		ctx,
		activity,
		chasm.TaskInvocation{TaskAttributes: chasm.TaskAttributes{ScheduledTime: defaultTime.Add(1500 * time.Millisecond)}},
		task,
	)
	require.NoError(t, err)
	require.False(t, valid)

	recordings := capture.Snapshot()[metrics.ActivityTaskTimeout.Name()]
	require.Len(t, recordings, 1)
	require.Equal(t, enumspb.TIMEOUT_TYPE_HEARTBEAT.String(), recordings[0].Tags["timeout_type"])
}

func ptr[T any](v T) *T { return &v }
