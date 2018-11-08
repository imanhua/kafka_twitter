import pycountry

place = "Thyroid-associated orbitopathy (TO) is an autoimmune-; mediated orbital inflammation that can Changchun lead to disfigurement and blindness.; Multiple genetic loci have been associated with Graves' disease, but the genetic; basis for TO is largely unknown. This study aimed to identify loci associated with; TO in individuals with Graves' disease, using a genome-wide association scan (GWAS) for the first time to our knowledge in TO.Genome-wide association scan was;China performed on pooled DNA from an Australian Caucasian discovery cohort of 265; participants with Graves' disease and TO (cases) and 147 patients with Graves' disease without TO (controls)."

for country in pycountry.countries:
    if country.name in place:
        print(country.name)
