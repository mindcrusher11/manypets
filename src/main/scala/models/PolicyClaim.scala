package org.manypets.cam
package models

import java.sql.Date


case class PolicyClaim(uuid_policy: String, date_of_loss: Date, policy_reference: Long,
                       claim_outcome: String,  payout: Double)
