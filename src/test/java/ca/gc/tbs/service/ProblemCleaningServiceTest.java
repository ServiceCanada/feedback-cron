package ca.gc.tbs.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ca.gc.tbs.domain.Problem;
import ca.gc.tbs.repository.ProblemRepository;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.LoggerFactory;

class ProblemCleaningServiceTest {

  private static final String RAW = "My name is John Smith, call me at 613-555-0199";
  private static final String CLEANED = "My name is ##########, call me at ###-###-####";

  private ContentService contentService;
  private ProblemRepository problemRepository;
  private SpreadsheetService spreadsheetService;
  private ProblemCleaningService service;
  private Logger logger;
  private ListAppender<ILoggingEvent> logs;

  @BeforeEach
  void setUp() {
    contentService = mock(ContentService.class);
    problemRepository = mock(ProblemRepository.class);
    spreadsheetService = mock(SpreadsheetService.class);
    service =
        new ProblemCleaningService(
            contentService, problemRepository, spreadsheetService, mock(BadWords.class));

    logger = (Logger) LoggerFactory.getLogger(ProblemCleaningService.class);
    logs = new ListAppender<>();
    logs.start();
    logger.addAppender(logs);
  }

  @AfterEach
  void tearDown() {
    logger.detachAppender(logs);
  }

  @Test
  void duplicateIsCleanedBeforeGoingToTheSpreadsheet() {
    when(contentService.cleanContent(RAW)).thenReturn(CLEANED);
    givenUncleaned(problem("1"), problem("2"));

    service.cleanProblems();

    ArgumentCaptor<Problem> duplicate = ArgumentCaptor.forClass(Problem.class);
    verify(spreadsheetService).logDuplicateComment(duplicate.capture());
    assertThat(duplicate.getValue().getId()).isEqualTo("2");
    assertThat(duplicate.getValue().getProblemDetails()).isEqualTo(CLEANED);
    assertNoCommentTextLogged();
  }

  @Test
  void failedCleaningDoesNotLogTheComment() {
    when(contentService.cleanContent(anyString())).thenThrow(new IllegalStateException("boom"));
    givenUncleaned(problem("1"));

    service.cleanProblems();

    assertThat(logs.list).anyMatch(e -> e.getFormattedMessage().contains("1"));
    assertNoCommentTextLogged();
  }

  @Test
  void blankInstitutionOnEnglishIrccUrlBecomesIrcc() {
    Problem problem = problem("1");
    problem.setInstitution("   ");
    String url =
        "https://WWW.CANADA.CA/en/Immigration-Refugees-Citizenship/services/visit-canada.html?utm_source=test#comments";
    problem.setUrl(url);

    Problem saved = cleanOne(problem);

    assertThat(saved.getInstitution()).isEqualTo("IRCC");
    assertThat(saved.getUrl()).isEqualTo(url);
    assertThat(saved.getProblemDetails()).isEqualTo(CLEANED);
    assertThat(saved.getPersonalInfoProcessed()).isEqualTo("true");
  }

  @Test
  void blankInstitutionOnFrenchIrccUrlBecomesIrcc() {
    Problem problem = problem("1");
    problem.setUrl(
        "https://www.canada.ca/fr/Immigration-Refugies-Citoyennete/services/visite.html");

    assertThat(cleanOne(problem).getInstitution()).isEqualTo("IRCC");
  }

  @Test
  void blankInstitutionOnEnglishCraUrlBecomesCra() {
    Problem problem = problem("1");
    problem.setUrl("https://www.canada.ca/en/Revenue-Agency.HTML");

    assertThat(cleanOne(problem).getInstitution()).isEqualTo("CRA");
  }

  @Test
  void blankInstitutionOnFrenchCraUrlBecomesCra() {
    Problem problem = problem("1");
    problem.setUrl("https://canada.ca/fr/agence-revenu/services/impot/particuliers.html");

    assertThat(cleanOne(problem).getInstitution()).isEqualTo("CRA");
  }

  @Test
  void knownDepartmentSlugBecomesItsAcronym() {
    when(contentService.cleanContent(anyString()))
        .thenAnswer(invocation -> invocation.getArgument(0));
    List<Problem> problems = new ArrayList<>();
    int id = 1;
    for (String[] department : DEPARTMENTS) {
      addDepartmentPage(problems, id++, "en", department[1]);
      addDepartmentPage(problems, id++, "fr", department[2]);
    }
    Problem globalAffairs = problem(Integer.toString(id));
    globalAffairs.setProblemDetails("Comment on the Global Affairs subdomain");
    globalAffairs.setUrl(
        "https://international.canada.ca/fr/affaires-mondiales/services/visas.html");
    problems.add(globalAffairs);
    givenUncleaned(problems.toArray(Problem[]::new));

    service.cleanProblems();

    for (int i = 0; i < DEPARTMENTS.length; i++) {
      assertThat(problems.get(i * 2).getInstitution())
          .as(DEPARTMENTS[i][1])
          .isEqualTo(DEPARTMENTS[i][0]);
      assertThat(problems.get(i * 2 + 1).getInstitution())
          .as(DEPARTMENTS[i][2])
          .isEqualTo(DEPARTMENTS[i][0]);
    }
    assertThat(globalAffairs.getInstitution()).isEqualTo("GAC");
    assertThat(problems).extracting(Problem::getPersonalInfoProcessed).containsOnly("true");
  }

  @Test
  void blankInstitutionOnDepartmentHostBecomesThatDepartment() {
    when(contentService.cleanContent(anyString()))
        .thenAnswer(invocation -> invocation.getArgument(0));
    String[][] pages = {
      {"https://ircc.canada.ca/english/helpcentre/index-featured-can.asp", "IRCC"},
      {
        "https://www.ircc.canada.ca/francais/centre-aide/index-en-vedette-can.asp?qnum=1#top",
        "IRCC"
      },
      {"https://IRCC.CANADA.CA/english/information/fees/fees.asp", "IRCC"},
      {"https://agriculture.canada.ca/en", "AAFC"},
      {"https://inspection.canada.ca/en/inspection-and-enforcement", "CFIA"},
      {"https://international.canada.ca/", "GAC"},
      {"https://ised-isde.canada.ca/site/ised/en", "ISED"},
      {"https://www.nrc.canada.ca/en", "NRC"},
      {"https://natural-resources.canada.ca/", "NRCAN"},
      {"https://nserc-crsng.canada.ca/en", "NSERC"},
    };
    List<Problem> problems = new ArrayList<>();
    int id = 1;
    for (String[] page : pages) {
      Problem problem = problem(Integer.toString(id++));
      problem.setProblemDetails("Comment on " + page[0]);
      problem.setUrl(page[0]);
      problems.add(problem);
    }
    givenUncleaned(problems.toArray(Problem[]::new));

    service.cleanProblems();

    for (int i = 0; i < pages.length; i++) {
      assertThat(problems.get(i).getInstitution()).as(pages[i][0]).isEqualTo(pages[i][1]);
      assertThat(problems.get(i).getUrl()).isEqualTo(pages[i][0]);
    }
    assertThat(problems).extracting(Problem::getPersonalInfoProcessed).containsOnly("true");
  }

  @Test
  void otherCanadaCaHostsStayBlank() {
    when(contentService.cleanContent(anyString()))
        .thenAnswer(invocation -> invocation.getArgument(0));
    String[] urls = {
      "https://tc.canada.ca/en/marine-transportation",
      "https://parks.canada.ca/",
      "https://justice.canada.ca/",
      "https://health.canada.ca/",
      "https://fin.canada.ca/",
      "https://budget.canada.ca/",
      "https://open.canada.ca/en",
      "https://canadabuys.canada.ca/en",
    };
    List<Problem> problems = new ArrayList<>();
    int id = 1;
    for (String url : urls) {
      Problem problem = problem(Integer.toString(id++));
      problem.setProblemDetails("Comment on " + url);
      problem.setUrl(url);
      problems.add(problem);
    }
    givenUncleaned(problems.toArray(Problem[]::new));

    service.cleanProblems();

    assertThat(problems).extracting(Problem::getInstitution).containsOnlyNulls();
    assertThat(problems).extracting(Problem::getUrl).containsExactly(urls);
    assertThat(problems).extracting(Problem::getPersonalInfoProcessed).containsOnly("true");
  }

  @Test
  void existingInstitutionOnDepartmentHostIsLeftUnchanged() {
    Problem problem = problem("1");
    problem.setInstitution("CRA");
    problem.setUrl("https://ircc.canada.ca/english/helpcentre/index-featured-can.asp");

    assertThat(cleanOne(problem).getInstitution()).isEqualTo("CRA");
  }

  @Test
  void serviceCanadaPageUsesTheEsdcSlug() {
    Problem problem = problem("1");
    problem.setUrl(
        "https://www.canada.ca/en/employment-social-development/corporate/portfolio/service-canada.html");

    assertThat(cleanOne(problem).getInstitution()).isEqualTo("ESDC");
  }

  @Test
  void existingInstitutionIsLeftUnchanged() {
    Problem problem = problem("1");
    problem.setInstitution("CRA");
    problem.setUrl(
        "https://www.canada.ca/en/immigration-refugees-citizenship/services/visit-canada.html");

    Problem saved = cleanOne(problem);

    assertThat(saved.getInstitution()).isEqualTo("CRA");
    assertThat(saved.getPersonalInfoProcessed()).isEqualTo("true");
  }

  @Test
  void servicesAndHomepageUrlsStayBlank() {
    Problem services = problem("1");
    String servicesUrl =
        "https://www.canada.ca/en/services/immigration-refugees-citizenship/revenue-agency.html";
    services.setUrl(servicesUrl);
    Problem home = problem("2");
    home.setProblemDetails("The homepage has no department");
    String homeUrl = "https://www.canada.ca/EN/";
    home.setUrl(homeUrl);
    when(contentService.cleanContent(anyString()))
        .thenAnswer(invocation -> invocation.getArgument(0));
    givenUncleaned(services, home);

    service.cleanProblems();

    ArgumentCaptor<Problem> saved = ArgumentCaptor.forClass(Problem.class);
    verify(problemRepository, times(2)).save(saved.capture());
    assertThat(saved.getAllValues()).extracting(Problem::getInstitution).containsOnlyNulls();
    assertThat(saved.getAllValues())
        .extracting(Problem::getPersonalInfoProcessed)
        .containsOnly("true");
    assertThat(services.getUrl()).isEqualTo(servicesUrl);
    assertThat(home.getUrl()).isEqualTo(homeUrl);
  }

  @Test
  void nullOrGarbageUrlStaysBlank() {
    Problem nullUrl = problem("1");
    nullUrl.setUrl(null);
    Problem blankUrl = problem("2");
    blankUrl.setProblemDetails("A blank URL is not a department");
    blankUrl.setUrl("  ");
    Problem garbage = problem("3");
    garbage.setProblemDetails("A broken URL is not a department");
    String garbageUrl = "https://www.canada.ca/en/immigration refugees";
    garbage.setUrl(garbageUrl);
    Problem otherHost = problem("4");
    otherHost.setProblemDetails("Another site is not a canada.ca department");
    String otherHostUrl = "https://example.com/en/immigration-refugees-citizenship/services.html";
    otherHost.setUrl(otherHostUrl);
    Problem noLanguage = problem("5");
    noLanguage.setProblemDetails("A URL with no language segment is not a department");
    noLanguage.setUrl("https://www.canada.ca/immigration-refugees-citizenship.html");
    Problem tribunal = problem("6");
    tribunal.setProblemDetails("The social security tribunal site has no department slug");
    tribunal.setUrl("https://sst-tss.gc.ca/en/your-appeal");
    when(contentService.cleanContent(anyString()))
        .thenAnswer(invocation -> invocation.getArgument(0));
    givenUncleaned(nullUrl, blankUrl, garbage, otherHost, noLanguage, tribunal);

    service.cleanProblems();

    assertThat(nullUrl.getInstitution()).isNull();
    assertThat(blankUrl.getInstitution()).isNull();
    assertThat(garbage.getInstitution()).isNull();
    assertThat(otherHost.getInstitution()).isNull();
    assertThat(noLanguage.getInstitution()).isNull();
    assertThat(tribunal.getInstitution()).isNull();
    assertThat(garbage.getUrl()).isEqualTo(garbageUrl);
    assertThat(otherHost.getUrl()).isEqualTo(otherHostUrl);
    assertThat(List.of(nullUrl, blankUrl, garbage, otherHost, noLanguage, tribunal))
        .extracting(Problem::getPersonalInfoProcessed)
        .containsOnly("true");
  }

  @Test
  void junkCommentIsStillDeleted() {
    Problem junk = problem("1");
    junk.setUrl(
        "https://www.canada.ca/en/immigration-refugees-citizenship/services/visit-canada.html");
    junk.setProblemDetails("John 613-555-0199 " + "x".repeat(301));
    givenUncleaned(junk);

    service.cleanProblems();

    verify(problemRepository).delete(junk);
    verify(problemRepository, never()).save(any());
    assertThat(junk.getInstitution()).isNull();
    assertNoCommentTextLogged();
  }

  @Test
  void duplicateOnDepartmentPageIsStillDeleted() {
    when(contentService.cleanContent(RAW)).thenReturn(CLEANED);
    String url =
        "https://www.canada.ca/en/immigration-refugees-citizenship/services/visit-canada.html";
    Problem first = problem("1");
    first.setUrl(url);
    Problem second = problem("2");
    second.setUrl(url);
    givenUncleaned(first, second);

    service.cleanProblems();

    verify(problemRepository).delete(second);
    assertThat(second.getInstitution()).isNull();
    ArgumentCaptor<Problem> saved = ArgumentCaptor.forClass(Problem.class);
    verify(problemRepository).save(saved.capture());
    assertThat(saved.getValue().getId()).isEqualTo("1");
    assertThat(saved.getValue().getInstitution()).isEqualTo("IRCC");
    assertNoCommentTextLogged();
  }

  private static final String[][] DEPARTMENTS = {
    {"AAFC", "agriculture-agri-food", "agriculture-agroalimentaire"},
    {"CBSA", "border-services-agency", "agence-services-frontaliers"},
    {"CCG", "canadian-coast-guard", "garde-cotiere-canadienne"},
    {"CER", "canada-energy-regulator", "regie-energie-canada"},
    {"CFIA", "food-inspection-agency", "agence-inspection-aliments"},
    {
      "CIRNAC",
      "crown-indigenous-relations-northern-affairs",
      "relations-couronne-autochtones-affaires-nord"
    },
    {"CRA", "revenue-agency", "agence-revenu"},
    {"CSC", "correctional-service", "service-correctionnel"},
    {"CSE", "communications-security", "securite-telecommunications"},
    {"DFO", "fisheries-oceans", "peches-oceans"},
    {"DND", "department-national-defence", "ministere-defense-nationale"},
    {"ECCC", "environment-climate-change", "environnement-changement-climatique"},
    {"ESDC", "employment-social-development", "emploi-developpement-social"},
    {"FCAC", "financial-consumer-agency", "agence-consommation-matiere-financiere"},
    {"FIN", "department-finance", "ministere-finances"},
    {"GAC", "global-affairs", "affaires-mondiales"},
    {"HC", "health-canada", "sante-canada"},
    {"HICC", "housing-infrastructure-communities", "logement-infrastructures-collectivites"},
    {"IRCC", "immigration-refugees-citizenship", "immigration-refugies-citoyennete"},
    {"ISC", "indigenous-services-canada", "services-autochtones-canada"},
    {
      "ISED",
      "innovation-science-economic-development",
      "innovation-sciences-developpement-economique"
    },
    {"LAC", "library-archives", "bibliotheque-archives"},
    {"NRC", "national-research-council", "conseil-national-recherches"},
    {"NRCAN", "natural-resources-canada", "ressources-naturelles-canada"},
    {"NSERC", "science-engineering-research", "recherches-sciences-genie"},
    {"OMBDNDCAF", "ombudsman-national-defence-forces", "ombudsman-defense-nationale-forces"},
    {"PS", "public-safety-canada", "securite-publique-canada"},
    {"PSPC", "public-services-procurement", "services-publics-approvisionnement"},
    {"PSPC-OL", "translation-bureau", "bureau-traduction"},
    {"PHAC", "public-health", "sante-publique"},
    {"SSHRC", "social-sciences-humanities-research", "recherches-sciences-humaines"},
    {"TBS", "treasury-board-secretariat", "secretariat-conseil-tresor"},
  };

  private static void addDepartmentPage(
      List<Problem> problems, int id, String language, String slug) {
    Problem problem = problem(Integer.toString(id));
    problem.setProblemDetails("Comment " + id + " about " + slug);
    problem.setUrl("https://www.canada.ca/" + language + "/" + slug + "/services/example.html");
    problems.add(problem);
  }

  private Problem cleanOne(Problem problem) {
    when(contentService.cleanContent(RAW)).thenReturn(CLEANED);
    givenUncleaned(problem);
    service.cleanProblems();
    ArgumentCaptor<Problem> saved = ArgumentCaptor.forClass(Problem.class);
    verify(problemRepository).save(saved.capture());
    assertNoCommentTextLogged();
    return saved.getValue();
  }

  private void givenUncleaned(Problem... problems) {
    when(problemRepository.findByPersonalInfoProcessed(null))
        .thenReturn(new ArrayList<>(List.of(problems)));
    when(problemRepository.findByPersonalInfoProcessed("false")).thenReturn(new ArrayList<>());
    when(problemRepository.save(any(Problem.class))).thenAnswer(i -> i.getArgument(0));
  }

  private void assertNoCommentTextLogged() {
    assertThat(logs.list)
        .extracting(ILoggingEvent::getFormattedMessage)
        .noneMatch(m -> m.contains("John") || m.contains("613-555"));
  }

  private static Problem problem(String id) {
    Problem p = new Problem();
    p.setId(id);
    p.setProblemDetails(RAW);
    p.setUrl("https://www.canada.ca/en/services/benefits.html");
    return p;
  }
}
