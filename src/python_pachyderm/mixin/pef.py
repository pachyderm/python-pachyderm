def _walk_input_names(input):
    if input is None:
        return
    if input.pfs:
        yield input.pfs.repo
    if input.cron:
        yield input.cron.repo
    if input.union:
        for sub_input in input.union:
            yield from _walk_input_names(sub_input)
    if input.cross:
        for sub_input in input.cross:
            yield from _walk_input_names(sub_input)
    if input.join:
        for sub_input in input.join:
            yield from _walk_input_names(sub_input)


class Experiment:
    def __init__(self, name):
        self.name = name
        self.version = 1
        self._pipelines = {}

    def add_pipeline(self, pipeline_name, transform, **kwargs):
        if not transform.env:
            transform.env = {} 
        transform.env["PACHYDERM_EXPERIMENT_NAME"] = self.name
        self._pipelines[pipeline_name] = (transform, kwargs)

    def validate(self):
        if self.name not in self._pipelines:
            raise Exception("a pipeline with the same name as the experiment ('{}') should be included".format(self.name))

        covered = set(self.name)
        crawl = []
        i = 0

        while i < len(crawl):
            pipeline_name = crawl[i]
            i += 1
            if pipeline_name in covered:
                continue

            pipeline_spec = self._pipelines.get(pipeline_name)
            if pipeline_spec is None:
                # this input is something outside of the experiment
                continue

            dependents = list(_walk_input_names(pipeline_spec[2].get("input")))
            if self.name in dependents:
                raise Exception("pipeline '{}': depends on experiment output pipeline ('{}')".format(pipeline_name, self.name))

            crawl.extend(dependents)
            covered.add(pipeline_name)

        divorced_pipeline_names = set(self._pipelines.keys()) - covered
        if divorced_pipeline_names:
            raise Exception("some pipelines aren't connected to the experiment output pipeline: {}".format(", ".join(divorced_pipeline_names)))


class PEFMixin:
    def list_experiments(self, history=None):
        raise NotImplementedError()

    def create_experiment(self, experiment):
        experiment.validate()

        for (pipeline_name, (transform, kwargs)) in experiment._pipelines.items():
            self.create_pipeline(pipeline_name, transform, **kwargs)

    def delete_experiment(self, name):
        raise NotImplementedError()
