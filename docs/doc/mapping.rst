


Entrez provides an Ensembl-to-Entrez mapping file
Ensembl provides an Ensembl-to-Entrez mapping file

There could be mulitple associations:

- 1 Entrez to 1 Ensembl. Ex: [...]
- 1 Entrez to many Ensembl. Ex: [...]
- Many Entrez to 1 Ensembl. Ex: [...]
- Many Entrez to many Ensembl. Ex: [...]

We build an extra mapping file based on Entrez's and Ensembl's mapping files along
with some rules in order to both
- enrich existing mappings
- clean unvalid mappings

Rules are:
- we trust Entrez more: if Ensembl says 1 Ensembl gene is mapped to 2 Entrez genes, but Entrez says only one
  we will remove the other one. Ex: [...]
- if 1 Ensembl gene is mapped to 2 Entrez genes, but there's no Entrez mappings found, we don't discard the mapping
  but rather check whether this association is valid considering the symbol name. If Ensembl gene's symbol can be found
  as exactly the same as symbols found in Entrez, we keep association, otherwise we discard it.
  Ex: [...]

So be it.
