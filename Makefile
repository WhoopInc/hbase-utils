DISTDIR = bin
SRCDIR = src
CLASSES = $(DISTDIR)/TSDBTruncate.class

JC = javac
JFLAGS = -cp $(hbcp) -d $(DISTDIR)

all: $(CLASSES)

$(DISTDIR)/%.class: $(SRCDIR)/%.java
	mkdir -p $(@D)
	$(JC) $(JFLAGS) $<

clean:
	$(RM) -r $(DISTDIR)

.PHONY: clean
