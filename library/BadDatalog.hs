module BadDatalog where
import qualified Data.Text as T
import qualified Data.Set as S


newtype Var = Var Int
  deriving (Eq, Ord, Show)
newtype Tag = Tag T.Text
  deriving (Eq, Ord, Show)
data Clause = Clause Tag [Var]
data Rule = Rule Clause [Clause]
type Program = [Rule]


clauseVars :: Clause -> S.Set Var
clauseVars (Clause _ v) = S.fromList v

boundVars :: Rule -> S.Set Var
boundVars (Rule l _) = clauseVars l
