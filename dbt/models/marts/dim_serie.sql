select * from unnest([
    struct(432 as serie_id, 'Meta Selic' as nome, 'diária' as frequencia, '% a.a.' as unidade),
    struct(433, 'IPCA(variação mensal)', 'mensal', '%'),
    struct(1, 'Dólar comercial (venda)', 'diária', 'R$')
])