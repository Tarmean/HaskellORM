{-# LANGUAGE BangPatterns, TypeApplications #-}
module NFA where



import qualified Data.Vector.Unboxed as U
import qualified Data.Map as M
import qualified Data.IntMap.Strict as IM
import Control.Monad.State
import qualified Data.Vector as V
import qualified Data.IntSet as S
import Data.List (foldl')


newtype StateMap = StateMap (IM.IntMap Int)
 deriving Show
type StateVec = S.IntSet
newtype Pay = Pay (IM.IntMap Int)
 deriving Show

stepState :: StateMap -> StateVec -> StateVec
stepState (StateMap sm) = S.map (\i -> IM.findWithDefault 0 i sm)


stepPay :: Pay -> StateVec -> Int
stepPay (Pay p) = sum . S.toList . S.map (\i -> IM.findWithDefault 0 i p)


data PrefixTree = PrefixTree Int (M.Map Char PrefixTree) | Leaf Int
  deriving (Eq, Ord, Show)
type Id = Int
data Env = Env { edges :: M.Map Char (IM.IntMap Id), idGen :: Int, pay :: IM.IntMap Int }

genId :: State Env Id
genId = do
    e <- get
    let i = idGen e
    put $ e { idGen = i + 1 }
    return i

data StrictPair = StrictPair !Int !S.IntSet
doParse :: M.Map Char StateMap -> Pay -> String -> Int
doParse transitions pays s0 = case foldl' go (StrictPair 0 (S.singleton 0)) s0 of
  StrictPair s _ -> s
  where
    go :: StrictPair -> Char -> StrictPair
    go (StrictPair acc s) x = case transitions M.!? x of
      Nothing -> StrictPair acc (S.singleton 0)
      Just v -> 
        let !s' = stepState v s
        in StrictPair (acc + stepPay pays s') (S.insert 0 s')

genStates :: Id -> PrefixTree -> State Env ()
genStates curState (Leaf s) = modify $ \e -> e { pay = IM.insertWith (+) curState s (pay e) }
genStates curState (PrefixTree s m) = do
    modify $ \e -> e { pay = IM.insertWith (+) curState s (pay e) }
    forM_ (M.toList m) $ \(edge, inner) -> do
        child <- genId
        modify $ \e -> e { edges = M.insertWith IM.union edge (IM.singleton curState child) (edges e) }
        genStates child inner

genVecs :: PrefixTree -> (M.Map Char StateMap, Pay)
genVecs tree = (M.map StateMap $ edges env, Pay $ pay env)
  where env = execState (genStates 0 tree) (Env M.empty 1 IM.empty)
stateMaps :: [(String, Int)] -> PrefixTree
stateMaps = go
  where
    groupByFirst ls = M.fromListWith (<>) [(k, [(ks,i)]) | (k:ks, i) <- ls]
    payUp ls = sum [i | ("", i) <- ls]

    go [] = Leaf 0
    go ls = prefixTree (payUp ls) (M.filter (/= Leaf 0) $ M.map go (groupByFirst ls))
    prefixTree i ls
      | M.null ls = Leaf i
      | otherwise = PrefixTree i ls



main :: IO ()
main = do
   len <- readLn
   pairs <- getLine
   costs <- getLine
   putStrLn "1"
   let vpairs = V.fromListN len (words pairs)
       vcosts = U.fromListN len (map (read @Int) $ words costs)
       getSlice  i j = stateMaps $ zip (V.toList $ V.slice i (j-i+1) vpairs) (U.toList $ U.slice i (j-i+1) vcosts)
   len <- readLn @Int
   os <- replicateM_ len $ do
     putStrLn "1"
     [i,j,s] <- words <$> getLine
     let prefix = getSlice (read i) (read j)
     let (transitions, pays) = genVecs prefix 
     let !_ = doParse transitions pays s
     pure ()
   pure ()
   -- putStrLn $ show (minimum os) <> " " <> show (maximum os)


