


### Para utilizar:

### 1. Configure um arquivo .env na pasta principal  com os acessos abaixo:

HOST_RELACIONAMENTO= 
USER_RELACIONAMENTO= 
PASSWD_RELACIONAMENTO=  

CREDENTIALS_GOOGLE='credentials/credentials.json'
AU_GOOGLE='credentials/authorized_user.json'

ID_SHARED_DRIVE_GROWTH=0AFFpUVLt225rUk9PVA
ID_FOLDER_DATA_SCIENCE_HUB_DATA=1_fyNuVMAJ8iuB3-bmB85zHCpP5TwVN4-

DATA_PATH='data/'

### 2. Coloque os arquivos credentials.json e authorized_user.json dentro da pasta de credenciais, eles devem ter acesso para alterar e adentrar pasta GROWTH

## *Padrões de registros para branchs, commits e pull requests*
### Passo 1 - Nome da Branch
É importante identificar a tarefa. Aqui a simplicidade é importante. 
#### Padrão
id-do-card-kanbanize/super-resumo
#### Exemplo
git checkout -b ID-48131/fix-error-update (muda e ativa branch)

### Passo 2 - Padrões de Commit
Padrão algo como "tipo: descriçao".

- feat: Um novo recurso para a aplicação, e não precisa ser algo grande.
- fix: Correções de bugs.
- docs: Alterações em arquivos relacionados à documentações.
- perf: Alterações relacionadas à performance.
- test: Criação ou modificação de testes.
- chore: Alterações em arquivos de configuração.

#### Exemplo
perf: alteracao coleta de links de produto hrefs

### Passo 3 - Padrão de Título na Pull Request
Quando você executa um "Squash and Merge" dentro do GitHub por exemplo, é o título da sua pull request 
que fica como commit principal e dentro da mensagem do commit ficam os outros commits. 

#### Padrão
[id-do-card-kanbanize] tipo: descriçao

#### Exemplo
[ID-48131] perf: alteracao coleta de links de produto hrefs

#### __RESUMO-CASO-DE-USO__
#### 1. Criando uma nova branch
git checkout -b ID-[id-do-card-kanbanize]/super-resumo

#### 2. Commit com mensagem padronizada
git add .
git commit -m "tipo: descrição"

#### 3. Enviando a branch para o repositório remoto
git push origin ID-[id-do-card-kanbanize]/super-resumo

## *Informações do .gitignore*
Os .gitignore usados permitem que arquivos de logs, dados (temporários,csv), credenciais sejam ignorados, porém, 
as pastas se mantenham vazias mantendo a estrutura do projeto, mantendo os principais arquivos para funcionamento do script.

## *Dica de processo*

1. Crie uma nova branch a partir da main.
2. Realize alterações e faça commits.
3. Mantenha sua branch atualizada com as alterações da main.
4. Resolva conflitos, se necessário.
5. Finalize o trabalho mesclando sua branch na main.
6. Teste e empurre suas alterações para o repositório remoto.

Sobre o uso do Pull Requests: Se estiver colaborando com outras pessoas, 
considere usar pull requests (PRs) no GitHub para facilitar a revisão de código antes de mesclar alterações na main.
