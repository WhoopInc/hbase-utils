DISTDIR = bin
SRCDIR = src
CLASSES = $(DISTDIR)/TSDBTruncate.class

JC = javac
JFLAGS = -cp $(shell hbase classpath) -d $(DISTDIR)

all: $(CLASSES)

$(DISTDIR)/%.class: $(SRCDIR)/%.java
	mkdir -p $(@D)
	$(JC) $(JFLAGS) $<

clean:
	$(RM) -r $(DISTDIR)

.PHONY: clean
