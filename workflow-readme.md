# Contribuer

Après le clone : `make setup` (active le hook de vérification des commits).

## Règles d'or
1. Jamais de commit direct sur `main`.
2. Une branche par fonctionnalité : `type/description` (ex. `feat/client-tcp`).
3. Un commit = un changement logique, message `type(scope): résumé`.
4. Types : feat, fix, chore, refactor, test, docs, style, perf, build, ci, revert.
5. Pas de squash. Mise à jour de la branche avec `git rebase main`.
6. Merge dans main avec `git merge --no-ff`, après une CI verte (`make gitinette`).
7. Avant de push : `make gitinette`.