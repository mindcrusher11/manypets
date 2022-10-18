package org.manypets.cam
package models

import java.sql.{Date, Timestamp}

/**
  * model class for claims on policy
  *
  * @constructor uuid_policy, date_of_loss, policy_reference, claim_outcome, payout
  *
  * @author Gaurhari
  * */
case class PolicyClaim(uuid_policy: String,
                       date_of_loss: Timestamp,
                       policy_reference: Option[Long],
                       claim_outcome: String,
                       payout: Option[Double])
