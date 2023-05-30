module Hasql.Transaction.Private.Sessions where

import Hasql.Session
import Hasql.Transaction.Config
import Hasql.Transaction.Private.Prelude
import qualified Hasql.Transaction.Private.Statements as Statements
import qualified System.Random

{-
We may want to
do one transaction retry in case of the 23505 error, and fail if an identical
error is seen.
-}
inRetryingTransaction :: IsolationLevel -> Mode -> Session (a, Bool) -> Bool -> Session a
inRetryingTransaction level mode session preparable =
  (fix $ \retry attempt -> do
    attemptRes <- tryTransaction level mode session preparable
    case attemptRes of
      Just a -> return a
      Nothing -> do
        -- immediately retrying is not a good idea, because it may cause a
        -- thundering herd problem.  Instead, we wait a random amount of time
        -- before retrying.
        delay <- liftIO $ System.Random.randomRIO (3000, attempt * 3)
        liftIO $ threadDelay (min 30000 delay)
        retry delay
    ) 0

tryTransaction :: IsolationLevel -> Mode -> Session (a, Bool) -> Bool -> Session (Maybe a)
tryTransaction level mode body preparable = do
  statement () (Statements.beginTransaction level mode preparable)

  bodyRes <- catchError (fmap Just body) $ \error -> do
    statement () (Statements.abortTransaction preparable)
    handleTransactionError error $ return Nothing

  case bodyRes of
    Just (res, commit) -> catchError (commitOrAbort commit preparable $> Just res) $ \error -> do
      handleTransactionError error $ return Nothing
    Nothing -> return Nothing

commitOrAbort commit preparable =
  if commit
    then statement () (Statements.commitTransaction preparable)
    else statement () (Statements.abortTransaction preparable)

handleTransactionError error onTransactionError = case error of
  QueryError _ _ (ResultError (ServerError "40001" _ _ _ _)) -> onTransactionError
  error -> throwError error
