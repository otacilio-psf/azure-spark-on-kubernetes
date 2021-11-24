-- Qual era o número de alunos de cursos de Filosofia inscritos no ENADE 2017?
SELECT COUNT(1)
FROM gold.enade
WHERE CO_GRUPO_DESC like 'Filosofia%'

-- Qual é o número de alunos dos cursos de Filosofia do sexo Masculino?
SELECT COUNT(1)
FROM gold.enade
WHERE CO_GRUPO_DESC like 'Filosofia%' AND TP_SEXO = 'M'

-- Qual é o código de UF que possui a maior média de nota geral entre os alunos dos cursos de Filosofia?
SELECT CO_UF_CURSO, UF_SIGLA, AVG(NT_GER) AS MEDIA_GER
FROM gold.enade
WHERE CO_GRUPO_DESC like 'Filosofia%'
GROUP BY CO_UF_CURSO, UF_SIGLA
ORDER BY AVG(NT_GER) DESC

-- Qual é a diferença das médias de idade (arredondado para 2 dígitos decimais) entre os alunos do sexo masculino e feminino, dos cursos de filosofia?
SELECT TP_SEXO, AVG(CAST(NU_IDADE AS FLOAT)) AS MEDIA_IDADE
FROM gold.enade
WHERE CO_GRUPO_DESC like 'Filosofia%'
GROUP BY TP_SEXO

SELECT (34.5360465116279 - 33.176483564968) AS DIFF

-- Qual é a média da nota geral para alunos de filosofia, cujo curso está no estado (UF) de código 43?
SELECT CO_UF_CURSO, UF_SIGLA, AVG(NT_GER) AS MEDIA_GER
FROM gold.enade
WHERE CO_GRUPO_DESC like 'Filosofia%' AND CO_UF_CURSO=43
GROUP BY CO_UF_CURSO, UF_SIGLA

-- Qual é o código do estado brasileiro que possui a maior média de idade de alunos de filosofia?
SELECT CO_UF_CURSO, UF_SIGLA, AVG(CAST(NU_IDADE AS FLOAT)) AS MEDIA_IDADE
FROM gold.enade
WHERE CO_GRUPO_DESC like 'Filosofia%'
GROUP BY CO_UF_CURSO, UF_SIGLA
ORDER BY AVG(CAST(NU_IDADE AS FLOAT)) DESC
