{-# LANGUAGE ApplicativeDo #-}
module Test where
import Types

-- aQuery :: (ORM m) => m (Int, String, [(Int, Int)])
-- aQuery = runNameGen $ from users $ \u -> do
--     limit 10
--     name <- col u userFirstName
--     uid <- col u userId
--     js <- nested (userJobs u) $ \j -> do
--         jid <- col j jobId
--         ju <- nested (jobUser j) $ \u2 -> col u2 userId
--         return (jid, head ju)
--     pure (uid, name, js)
