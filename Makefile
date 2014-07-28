DISTDIR = dist
CLASSES = $(DISTDIR)/HBaseTruncate.class

JC = javac
JFLAGS = -cp $(shell hbase classpath) -d $(DISTDIR)

all: $(CLASSES)

$(DISTDIR)/%.class: %.java
	mkdir -p $(@D)
	$(JC) $(JFLAGS) $<

clean:
	$(RM) -r $(DISTDIR)

.PHONY: clean
