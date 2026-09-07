---
name: event-window-analysis
description: Analyze accepted pre-event, event, and post-event nighttime-light windows while preserving temporal and non-attribution limits.
---

# Event-Window Analysis

- Require an accepted EventContext and ObservationPackage; do not reconstruct event facts or retrieve replacement observations.
- Record local-night/UTC mapping, baseline and comparison windows, controls or references, valid-day criteria, and coverage.
- Compare absolute and percentage changes with low-radiance, cloud/quality, background-activity, seasonal, and buffer-dilution checks.
- Treat the result as a candidate signal consistent or inconsistent with the specified event context, not proof of cause, damage, or responsibility.
- Preserve alternative explanations and return upstream revision needs through NTL_Engineer.

