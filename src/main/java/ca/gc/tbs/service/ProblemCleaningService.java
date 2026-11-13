package ca.gc.tbs.service;

import ca.gc.tbs.domain.Problem;
import ca.gc.tbs.repository.ProblemRepository;
import ca.gc.tbs.util.ValidationUtils;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/** Service for cleaning Problem data. */
@Service
public class ProblemCleaningService {
  private static final Logger logger = LoggerFactory.getLogger(ProblemCleaningService.class);

  private static final int MAX_COMMENT_LENGTH = 301;

  // English and French first path segment. Values are option values the viewer already filters on.
  private static final Map<String, String> DEPARTMENT_BY_SLUG =
      Map.ofEntries(
          Map.entry("agriculture-agri-food", "AAFC"),
          Map.entry("agriculture-agroalimentaire", "AAFC"),
          Map.entry("border-services-agency", "CBSA"),
          Map.entry("agence-services-frontaliers", "CBSA"),
          Map.entry("canadian-coast-guard", "CCG"),
          Map.entry("garde-cotiere-canadienne", "CCG"),
          Map.entry("canada-energy-regulator", "CER"),
          Map.entry("regie-energie-canada", "CER"),
          Map.entry("food-inspection-agency", "CFIA"),
          Map.entry("agence-inspection-aliments", "CFIA"),
          Map.entry("crown-indigenous-relations-northern-affairs", "CIRNAC"),
          Map.entry("relations-couronne-autochtones-affaires-nord", "CIRNAC"),
          Map.entry("revenue-agency", "CRA"),
          Map.entry("agence-revenu", "CRA"),
          Map.entry("correctional-service", "CSC"),
          Map.entry("service-correctionnel", "CSC"),
          Map.entry("communications-security", "CSE"),
          Map.entry("securite-telecommunications", "CSE"),
          Map.entry("fisheries-oceans", "DFO"),
          Map.entry("peches-oceans", "DFO"),
          Map.entry("department-national-defence", "DND"),
          Map.entry("ministere-defense-nationale", "DND"),
          Map.entry("environment-climate-change", "ECCC"),
          Map.entry("environnement-changement-climatique", "ECCC"),
          Map.entry("employment-social-development", "ESDC"),
          Map.entry("emploi-developpement-social", "ESDC"),
          Map.entry("financial-consumer-agency", "FCAC"),
          Map.entry("agence-consommation-matiere-financiere", "FCAC"),
          Map.entry("department-finance", "FIN"),
          Map.entry("ministere-finances", "FIN"),
          Map.entry("global-affairs", "GAC"),
          Map.entry("affaires-mondiales", "GAC"),
          Map.entry("health-canada", "HC"),
          Map.entry("sante-canada", "HC"),
          Map.entry("housing-infrastructure-communities", "HICC"),
          Map.entry("logement-infrastructures-collectivites", "HICC"),
          Map.entry("immigration-refugees-citizenship", "IRCC"),
          Map.entry("immigration-refugies-citoyennete", "IRCC"),
          Map.entry("indigenous-services-canada", "ISC"),
          Map.entry("services-autochtones-canada", "ISC"),
          Map.entry("innovation-science-economic-development", "ISED"),
          Map.entry("innovation-sciences-developpement-economique", "ISED"),
          Map.entry("library-archives", "LAC"),
          Map.entry("bibliotheque-archives", "LAC"),
          Map.entry("national-research-council", "NRC"),
          Map.entry("conseil-national-recherches", "NRC"),
          Map.entry("natural-resources-canada", "NRCAN"),
          Map.entry("ressources-naturelles-canada", "NRCAN"),
          Map.entry("science-engineering-research", "NSERC"),
          Map.entry("recherches-sciences-genie", "NSERC"),
          Map.entry("ombudsman-national-defence-forces", "OMBDNDCAF"),
          Map.entry("ombudsman-defense-nationale-forces", "OMBDNDCAF"),
          Map.entry("public-safety-canada", "PS"),
          Map.entry("securite-publique-canada", "PS"),
          Map.entry("public-services-procurement", "PSPC"),
          Map.entry("services-publics-approvisionnement", "PSPC"),
          Map.entry("translation-bureau", "PSPC-OL"),
          Map.entry("bureau-traduction", "PSPC-OL"),
          Map.entry("public-health", "PHAC"),
          Map.entry("sante-publique", "PHAC"),
          Map.entry("social-sciences-humanities-research", "SSHRC"),
          Map.entry("recherches-sciences-humaines", "SSHRC"),
          Map.entry("treasury-board-secretariat", "TBS"),
          Map.entry("secretariat-conseil-tresor", "TBS"));

  private final ContentService contentService;
  private final ProblemRepository problemRepository;
  private final SpreadsheetService spreadsheetService;

  @Autowired
  public ProblemCleaningService(
      ContentService contentService,
      ProblemRepository problemRepository,
      SpreadsheetService spreadsheetService,
      BadWords badWords) {
    // Load BadWords config (JAR uses javax.annotation.PostConstruct which Spring Boot 3.x ignores)
    badWords.loadConfigs();

    this.contentService = contentService;
    this.problemRepository = problemRepository;
    this.spreadsheetService = spreadsheetService;
  }

  /**
   * Cleans all unprocessed Problem entries. Removes junk/duplicates and cleans personal info from
   * valid records.
   */
  public void cleanProblems() {
    Set<String> seenComments = new HashSet<>();
    List<Problem> problems = fetchUncleanedProblems();
    logger.info("Number of Problems to clean: {}", problems.size());

    for (Problem problem : problems) {
      try {
        processProblem(problem, seenComments);
      } catch (Exception e) {
        logger.error("Could not process problem: {}", problem.getId(), e);
      }
    }
    logger.info("Problem cleaning complete");
  }

  private List<Problem> fetchUncleanedProblems() {
    List<Problem> problems = problemRepository.findByPersonalInfoProcessed(null);
    problems.addAll(problemRepository.findByPersonalInfoProcessed("false"));
    return problems;
  }

  private void processProblem(Problem problem, Set<String> seenComments) {
    // Check for junk first - delete immediately without wasting time cleaning
    if (isJunkComment(problem)) {
      logger.info("Deleting junk comment: {}", problem.getId());
      problemRepository.delete(problem);
      return;
    }

    // Check for duplicates within this batch
    String normalizedComment = problem.getProblemDetails().trim().toLowerCase();
    if (ValidationUtils.isDuplicateComment(normalizedComment, seenComments)) {
      logger.info("Deleting duplicate comment: {}", problem.getId());
      problem.setProblemDetails(contentService.cleanContent(problem.getProblemDetails()));
      spreadsheetService.logDuplicateComment(problem);
      problemRepository.delete(problem);
      return;
    }
    seenComments.add(normalizedComment);

    // Before save, so Mongo and the later Airtable copy both get the department.
    fillBlankInstitution(problem);

    // Clean personal info from valid, non-duplicate records
    String details = contentService.cleanContent(problem.getProblemDetails());
    problem.setProblemDetails(details);
    problem.setPersonalInfoProcessed("true");
    problemRepository.save(problem);
  }

  private void fillBlankInstitution(Problem problem) {
    String institution = problem.getInstitution();
    if (institution != null && !institution.isBlank()) {
      return;
    }
    try {
      String acronym = departmentFromUrl(problem.getUrl());
      if (acronym != null) {
        problem.setInstitution(acronym);
      }
    } catch (RuntimeException e) {
      // Leave the department blank so cleaning this comment still finishes.
      logger.debug("Could not read a department for problem {}", problem.getId(), e);
    }
  }

  private static String departmentFromUrl(String url) {
    if (url == null || url.isBlank()) {
      return null;
    }
    URI uri;
    try {
      uri = URI.create(url.trim());
    } catch (IllegalArgumentException e) {
      return null;
    }
    String host = uri.getHost();
    if (host == null) {
      return null;
    }
    host = host.toLowerCase(Locale.ROOT);
    // Institution pages also live on subdomains such as international.canada.ca.
    if (!"canada.ca".equals(host) && !host.endsWith(".canada.ca")) {
      return null;
    }
    String path = uri.getPath();
    if (path == null) {
      return null;
    }

    String language = null;
    String slug = null;
    for (String segment : path.split("/")) {
      if (segment.isEmpty()) {
        continue;
      }
      if (language == null) {
        language = segment.toLowerCase(Locale.ROOT);
      } else {
        slug = segment.toLowerCase(Locale.ROOT);
        break;
      }
    }
    if (slug == null || (!"en".equals(language) && !"fr".equals(language))) {
      return null;
    }
    // Home pages are "{slug}.html", so drop the suffix before matching.
    if (slug.endsWith(".html")) {
      slug = slug.substring(0, slug.length() - ".html".length());
    }
    return DEPARTMENT_BY_SLUG.get(slug);
  }

  private boolean isJunkComment(Problem problem) {
    String details = problem.getProblemDetails();
    String url = problem.getUrl();
    return details.trim().isEmpty()
        || ValidationUtils.containsHTML(details)
        || "https://www.canada.ca/".equals(url)
        || details.length() > MAX_COMMENT_LENGTH;
  }
}
