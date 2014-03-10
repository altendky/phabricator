<?php

/**
 * @group conduit
 */
final class ConduitAPI_diffusion_diffquery_Method
  extends ConduitAPI_diffusion_abstractquery_Method {

  private $effectiveCommit;

  public function getMethodDescription() {
    return
      'Get diff information from a repository for a specific path at an '.
      '(optional) commit.';
  }

  public function defineReturnType() {
    return 'array';
  }

  protected function defineCustomParamTypes() {
    return array(
      'path' => 'required string',
      'commit' => 'optional string',
    );
  }

  protected function getResult(ConduitAPIRequest $request) {
    $result = parent::getResult($request);

    return array(
      'changes' => mpull($result, 'toDictionary'),
      'effectiveCommit' => $this->getEffectiveCommit($request));
  }

  protected function getGitResult(ConduitAPIRequest $request) {
    return $this->getGitOrMercurialResult($request);
  }

  protected function getMercurialResult(ConduitAPIRequest $request) {
    return $this->getGitOrMercurialResult($request);
  }

  /**
   * NOTE: We have to work particularly hard for SVN as compared to other VCS.
   * That's okay but means this shares little code with the other VCS.
   */
  protected function getSVNResult(ConduitAPIRequest $request) {
    $drequest = $this->getDiffusionRequest();
    $repository = $drequest->getRepository();

    $effective_commit = $this->getEffectiveCommit($request);
    if (!$effective_commit) {
      return $this->getEmptyResult();
    }

    $drequest = clone $drequest;
    $drequest->setCommit($effective_commit);

    $path_change_query = DiffusionPathChangeQuery::newFromDiffusionRequest(
      $drequest);
    $path_changes = $path_change_query->loadChanges();

    $path = null;
    foreach ($path_changes as $change) {
      if ($change->getPath() == $drequest->getPath()) {
        $path = $change;
      }
    }

    if (!$path) {
      return $this->getEmptyResult();
    }

    $change_type = $path->getChangeType();
    switch ($change_type) {
      case DifferentialChangeType::TYPE_MULTICOPY:
      case DifferentialChangeType::TYPE_DELETE:
        if ($path->getTargetPath()) {
          $old = array(
            $path->getTargetPath(),
            $path->getTargetCommitIdentifier());
        } else {
          $old = array($path->getPath(), $path->getCommitIdentifier() - 1);
        }
        $old_name = $path->getPath();
        $new_name = '';
        $new = null;
        break;
      case DifferentialChangeType::TYPE_ADD:
        $old = null;
        $new = array($path->getPath(), $path->getCommitIdentifier());
        $old_name = '';
        $new_name = $path->getPath();
        break;
      case DifferentialChangeType::TYPE_MOVE_HERE:
      case DifferentialChangeType::TYPE_COPY_HERE:
        $old = array(
          $path->getTargetPath(),
          $path->getTargetCommitIdentifier());
        $new = array($path->getPath(), $path->getCommitIdentifier());
        $old_name = $path->getTargetPath();
        $new_name = $path->getPath();
        break;
      case DifferentialChangeType::TYPE_MOVE_AWAY:
        $old = array(
          $path->getPath(),
          $path->getCommitIdentifier() - 1);
        $old_name = $path->getPath();
        $new_name = null;
        $new = null;
        break;
      default:
        $old = array($path->getPath(), $path->getCommitIdentifier() - 1);
        $new = array($path->getPath(), $path->getCommitIdentifier());
        $old_name = $path->getPath();
        $new_name = $path->getPath();
        break;
    }

    $proplistFutures = array(
      'old' => $this->buildSVNFuture($old, 'proplist'),
      'new' => $this->buildSVNFuture($new, 'proplist'),
    );
    $proplistFutures = array_filter($proplistFutures);
    $proplistFutures = $this->execFutures($proplistFutures, $path);

    $proplistFutures['old'] = explode("\n", idx($proplistFutures, 'old', ''));
    $proplistFutures['old'] = array_slice($proplistFutures['old'], 1);
    $proplistFutures['old'] = array_map('trim', $proplistFutures['old']);
    $proplistFutures['old'] = array_filter($proplistFutures['old']);
    $proplistFutures['new'] = explode("\n", idx($proplistFutures, 'new', ''));
    $proplistFutures['new'] = array_slice($proplistFutures['new'], 1);
    $proplistFutures['new'] = array_map('trim', $proplistFutures['new']);
    $proplistFutures['new'] = array_filter($proplistFutures['new']);

    $contentFutures = array(
      'old' => $this->buildSVNFuture($old, 'cat'),
      'new' => $this->buildSVNFuture($new, 'cat'),
    );
    $contentFutures = array_filter($contentFutures);
    $contentFutures = $this->execFutures($contentFutures, $path);

    $propgetFutures = array(
      'old' => array(),
      'new' => array(),
    );
    foreach ($proplistFutures['old'] as $p) {
      $propgetFutures['old'][$p] = $this->buildSVNFuture($old, 'propget '.$p);
    }
    foreach ($proplistFutures['new'] as $p) {
      $propgetFutures['new'][$p] = $this->buildSVNFuture($new, 'propget '.$p);
    }
    $propgetFutures['old'] = array_filter($propgetFutures['old']);
    $propgetFutures['old'] = $this->execFutures($propgetFutures['old'], $path);
    $propgetFutures['new'] = array_filter($propgetFutures['new']);
    $propgetFutures['new'] = $this->execFutures($propgetFutures['new'], $path);

    $old_data = idx($contentFutures, 'old', '');
    $new_data = idx($contentFutures, 'new', '');

    $engine = new PhabricatorDifferenceEngine();
    $engine->setOldName($old_name);
    $engine->setNewName($new_name);
    $raw_diff = $engine->generateRawDiffFromFileContent($old_data, $new_data);

    $arcanist_changes = DiffusionPathChange::convertToArcanistChanges(
      $path_changes);

    $parser = $this->getDefaultParser();
    $parser->setChanges($arcanist_changes);
    $parser->forcePath($path->getPath());
    $changes = $parser->parseDiff($raw_diff);

    $change = $changes[$path->getPath()];

    foreach ($propgetFutures['old'] as $key => $value) {
      $change->setOldProperty($key, $value);
    }
    foreach ($propgetFutures['new'] as $key => $value) {
      $change->setNewProperty($key, $value);
    }

    return array($change);
  }

  private function execFutures($futures, $path) {
    foreach (Futures($futures) as $key => $future) {
      $stdout = '';
      try {
        list($stdout) = $future->resolvex();
      } catch (CommandException $e) {
        if ($path->getFileType() != DifferentialChangeType::FILE_DIRECTORY) {
          throw $e;
        }
      }
      $futures[$key] = $stdout;
    }

    return $futures;
  }

  private function getEffectiveCommit(ConduitAPIRequest $request) {
    if ($this->effectiveCommit === null) {
      $drequest = $this->getDiffusionRequest();
      $user = $request->getUser();
      $commit = null;

      $conduit_result = DiffusionQuery::callConduitWithDiffusionRequest(
        $user,
        $drequest,
        'diffusion.lastmodifiedquery',
        array(
          'commit' => $drequest->getCommit(),
          'path' => $drequest->getPath()));
      $c_dict = $conduit_result['commit'];
      if ($c_dict) {
        $commit = PhabricatorRepositoryCommit::newFromDictionary($c_dict);
      }
      if (!$commit) {
        // TODO: Improve error messages here.
        return null;
      }
      $this->effectiveCommit = $commit->getCommitIdentifier();
    }
    return $this->effectiveCommit;
  }

  private function buildSVNFuture($spec, $command) {
    if (!$spec) {
      return null;
    }

    $drequest = $this->getDiffusionRequest();
    $repository = $drequest->getRepository();

    list($ref, $rev) = $spec;
    return $repository->getRemoteCommandFuture(
      $command.' %s',
      $repository->getSubversionPathURI($ref, $rev));
  }

  private function getGitOrMercurialResult(ConduitAPIRequest $request) {
    $drequest = $this->getDiffusionRequest();
    $repository = $drequest->getRepository();

    $effective_commit = $this->getEffectiveCommit($request);
    if (!$effective_commit) {
      return $this->getEmptyResult(1);
    }
    // TODO: This side effect is kind of sketchy.
    $drequest->setCommit($effective_commit);

    $raw_query = DiffusionRawDiffQuery::newFromDiffusionRequest($drequest);
    $raw_diff = $raw_query->loadRawDiff();
    if (!$raw_diff) {
      return $this->getEmptyResult(2);
    }

    $parser = $this->getDefaultParser();
    $changes = $parser->parseDiff($raw_diff);

    return $changes;
  }

  private function getDefaultParser() {
    $drequest = $this->getDiffusionRequest();
    $repository = $drequest->getRepository();

    $parser = new ArcanistDiffParser();
    $try_encoding = $repository->getDetail('encoding');
    if ($try_encoding) {
      $parser->setTryEncoding($try_encoding);
    }
    $parser->setDetectBinaryFiles(true);

    return $parser;
  }

  private function getEmptyResult() {
    return array();
  }

}
