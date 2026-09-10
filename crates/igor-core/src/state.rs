use serde::{Deserialize, Serialize};

use crate::{DomainError, ErrorCategory, ErrorCode, Result};

pub trait TransitionState: Copy + Sized + 'static {
    const ALL: &'static [Self];

    fn can_transition_to(self, next: Self) -> bool;
    fn as_str(self) -> &'static str;

    fn transition_to(self, next: Self) -> Result<Self> {
        if self.can_transition_to(next) {
            Ok(next)
        } else {
            Err(DomainError::InvalidTransition {
                category: ErrorCategory::StateTransition,
                code: ErrorCode::InvalidStateTransition,
                entity: std::any::type_name::<Self>().into(),
                from: self.as_str().into(),
                to: next.as_str().into(),
            })
        }
    }
}

macro_rules! state_machine {
    (
        $name:ident { $($state:ident => $text:literal),+ $(,)? }
        transitions { $( $from:ident => [$($to:ident),* $(,)?] ),+ $(,)? }
    ) => {
        #[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
        #[serde(rename_all = "snake_case")]
        pub enum $name { $($state),+ }

        impl TransitionState for $name {
            const ALL: &'static [Self] = &[$(Self::$state),+];

            fn can_transition_to(self, next: Self) -> bool {
                match self {
                    $(Self::$from => [$(Self::$to),*].contains(&next),)+
                }
            }

            fn as_str(self) -> &'static str {
                match self { $(Self::$state => $text),+ }
            }
        }
    };
}

state_machine!(
    JobState {
        Queued => "queued", Running => "running", Succeeded => "succeeded",
        Failed => "failed", Cancelled => "cancelled", Lost => "lost", Superseded => "superseded"
    }
    transitions {
        Queued => [Running, Cancelled, Superseded],
        Running => [Succeeded, Failed, Cancelled, Lost],
        Succeeded => [Superseded],
        Failed => [Queued, Superseded],
        Cancelled => [Queued, Superseded],
        Lost => [Queued, Superseded],
        Superseded => []
    }
);

state_machine!(
    AttemptState {
        Pending => "pending", Starting => "starting", Running => "running",
        Succeeded => "succeeded", Failed => "failed", Cancelled => "cancelled", Lost => "lost"
    }
    transitions {
        Pending => [Starting, Cancelled],
        Starting => [Running, Failed, Cancelled, Lost],
        Running => [Succeeded, Failed, Cancelled, Lost],
        Succeeded => [], Failed => [], Cancelled => [], Lost => []
    }
);

state_machine!(
    ActionState {
        Pending => "pending", Running => "running", Succeeded => "succeeded",
        Failed => "failed", Cancelled => "cancelled"
    }
    transitions {
        Pending => [Running, Cancelled], Running => [Succeeded, Failed, Pending, Cancelled],
        Succeeded => [], Failed => [Pending], Cancelled => []
    }
);

state_machine!(
    DeliveryState {
        Pending => "pending", Delivering => "delivering", Delivered => "delivered",
        Failed => "failed", Cancelled => "cancelled"
    }
    transitions {
        Pending => [Delivering, Cancelled], Delivering => [Delivered, Failed, Pending, Cancelled],
        Delivered => [], Failed => [Pending], Cancelled => []
    }
);

state_machine!(
    RecoveryState {
        Pending => "pending", Diagnosing => "diagnosing", Proposed => "proposed",
        Approved => "approved", Rejected => "rejected", Applying => "applying",
        Succeeded => "succeeded", Failed => "failed", Cancelled => "cancelled"
    }
    transitions {
        Pending => [Diagnosing, Cancelled], Diagnosing => [Proposed, Failed, Cancelled],
        Proposed => [Approved, Rejected, Cancelled], Approved => [Applying, Cancelled],
        Rejected => [], Applying => [Succeeded, Failed, Cancelled], Succeeded => [],
        Failed => [Pending], Cancelled => []
    }
);

state_machine!(
    ReportState {
        Pending => "pending", Generating => "generating", Published => "published",
        Failed => "failed", Cancelled => "cancelled", Superseded => "superseded"
    }
    transitions {
        Pending => [Generating, Cancelled], Generating => [Published, Failed, Pending, Cancelled],
        Published => [Superseded], Failed => [Pending], Cancelled => [], Superseded => []
    }
);

state_machine!(
    CleanupState {
        Pending => "pending", Running => "running", Succeeded => "succeeded",
        Failed => "failed", Cancelled => "cancelled"
    }
    transitions {
        Pending => [Running, Cancelled], Running => [Succeeded, Failed, Pending, Cancelled],
        Succeeded => [], Failed => [Pending], Cancelled => []
    }
);
