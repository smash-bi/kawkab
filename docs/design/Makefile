# A very preliminary makefile; will ``make" it more generic soon!

SEARCH_PATH = TEXINPUTS="../common:"
TEX = $(SEARCH_PATH) pdflatex -interatction=nonstopmode -output-directory out/
BASE = design-doc
SRC = $(BASE).tex
PDF = $(BASE).pdf

$(PDF) : $(SRC)
	$(TEX) $(SRC)

clean :
	rm out/*
